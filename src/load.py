from typing import Dict, List
import pandas as pd
import os
import datetime
from src.logging import start
from sqlalchemy import text


logger = start()


def if_table_choice(table: str, df: pd.DataFrame):
    matching = df[df["match_name_filtering"]]
    not_matching = df[~df["match_name_filtering"]]
    logger.info(f"{table.upper()} has {len(matching)} rows matching 'Política de Assinatura' or 'Signature Policy'.")
    if len(not_matching) != 0:
        logger.info(f"There {'is' if len(not_matching) == 1 else 'are'} {len(not_matching)} row{'s' if len(not_matching) != 1 else ''} not matching.")


def get_unique_filename(output_dir: str) -> str:
    date_str = datetime.datetime.now().strftime("%m_%d_%Y")
    version = 1
    while True:
        filename = f"loaded_data_{date_str}_v{version}.xlsx"
        output_path = os.path.join(output_dir, filename)
        if not os.path.exists(output_path):
            return output_path
        version += 1


def load(dataframes: Dict[str, pd.DataFrame], conn, new_db):
    logger.debug(f"Starting the loading process...")

    output_dir = "src/loaded"
    os.makedirs(output_dir, exist_ok=True)
    output_path = get_unique_filename(output_dir)
    
    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        try:
            for table, df in dataframes.items():
                logger.debug(f"Processing table '{table.upper()}' for Excel export...")
                if df.empty:
                    logger.warning(f"{table.upper()} is empty.")
                    continue

                logger.info(f"{table.upper()} extracted successfully with {len(df)} rows.")

                if table == "course":
                    prefixed_table = f"{new_db.prefix}_{table}"
                    context_table = f"{new_db.prefix}_context"
                    # migrating one course from old to new db
                    id = 52
                    course = df[df["id"] == id]
                    if course.empty:
                        logger.warning(f"No row(s) found in 'COURSE' with id {id}.")
                    else:
                        try:
                            course_copy = course.copy()
                            course_copy["category"] = 9
                            course_copy = course_copy.drop(columns=["id"])  # removing the old ID to generate a new one
                            course_copy = course_copy.drop(columns=["originalcourseid"])

                            # inserting the modified course into the new database (need to adjust the logic behind adding a new course into the new db; other important and relevant tables are not being copied yet.)
                            course_copy.to_sql(prefixed_table, conn, if_exists="append", index=False)
                            logger.info(f"COURSE based on ID {id} inserted successfully!")

                            # inserting a context into the new database
                            # Recuperar o novo ID do curso
                            result = conn.execute(text(f"SELECT id FROM {prefixed_table} ORDER BY id DESC LIMIT 1"))
                            new_course_id = result.scalar()
                            logger.debug(f"New course ID: {new_course_id}")

                            if not new_course_id:
                                raise ValueError("Failed to retrieve new course ID after insertion.")

                            category_id = course_copy["category"].iloc[0]
                            result = conn.execute(text(
                                f"""
                                SELECT id FROM {context_table}
                                WHERE contextlevel = 40 AND instanceid = {category_id}
                                LIMIT 1
                                """
                            ))
                            context_category_id = result.scalar()
                            logger.debug(f"Context category ID found: {context_category_id}")

                            if not context_category_id:
                                raise ValueError(f"No context category found for instanceid {category_id}.")

                            logger.debug("Inserting context row...")
                            conn.execute(text(
                                f"""
                                INSERT INTO {context_table} (contextlevel, instanceid, depth, path)
                                VALUES (50, {new_course_id}, 3, NULL)
                                """
                            ))

                            result = conn.execute(text(
                                f"""
                                SELECT id FROM {context_table}
                                WHERE contextlevel = 50 AND instanceid = {new_course_id}
                                ORDER BY id DESC LIMIT 1
                                """
                            ))
                            new_context_id = result.scalar()
                            logger.debug(f"New context ID for course: {new_context_id}")

                            path = f"/1/{context_category_id}/{new_context_id}"
                            conn.execute(text(
                                f"""
                                UPDATE {context_table}
                                SET path = '{path}'
                                WHERE id = {new_context_id}
                                """
                            ))
                            logger.info(f"Context inserted for course {new_course_id} with path '{path}'.")

                        except Exception as e:
                            logger.error(f"Error inserting copied COURSE based on ID {id}: {e}")

                if table == "choice":
                    if_table_choice(table, df)

                logger.info(f"{table.upper()} has {len(df.columns)} columns: {df.columns.tolist()}.")

                df.to_excel(writer, sheet_name=table, index=False)
            logger.info(f"File successfully saved: {os.path.basename(output_path)}.")
        except Exception as e:
            logger.error(f"Error creating the xlsx file: {e}.")
    logger.info(f"End of loading process.")
