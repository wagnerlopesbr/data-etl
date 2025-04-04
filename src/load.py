from typing import Dict, List
import pandas as pd
import os
import datetime
from src.logging import start


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


def load(dataframes: Dict[str, pd.DataFrame], new_engine, new_db):
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
                    # migrating one course from old to new db
                    id = 54
                    course = df[df["id"] == id]
                    if course.empty:
                        logger.warning(f"No row(s) found in 'COURSE' with id {id}.")
                    else:
                        try:
                            course_copy = course.copy()
                            course_copy["category"] = 10
                            course_copy = course_copy.drop(columns=["id"])  # removing the old ID to generate a new one

                            # inserting the modified course into the new database (need to adjust the logic behind adding a new course into the new db; other important and relevant tables are not being copied yet.)
                            course_copy.to_sql(prefixed_table, new_engine, if_exists="append", index=False)
                            logger.info(f"COURSE based on ID {id} inserted successfully!")
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
