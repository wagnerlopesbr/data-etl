from typing import Dict, List
import pandas as pd
import os
import datetime
from src.logging import start
from sqlalchemy import text
from src.transform import transform_sequence


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
    
    module_instance_mapping = {}

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
                    sections_table = f"{new_db.prefix}_course_sections"
                    modules_table = f"{new_db.prefix}_modules"
                    course_modules_table = f"{new_db.prefix}_course_modules"

                    id = 53
                    sections_df = dataframes.get("course_sections", pd.DataFrame())
                    course_sections_df = sections_df[sections_df["course"] == id].copy()
                    modules_df = dataframes.get("modules", pd.DataFrame())
                    course_modules_df = dataframes.get("course_modules", pd.DataFrame())
                    old_modules_map = dict(zip(modules_df["id"], modules_df["name"]))
                    old_modules_result = conn.execute(text(f"SELECT id, name FROM {modules_table}")).mappings()
                    new_modules_map = {row["name"]: row["id"] for row in old_modules_result}
                    course_modules_filtered_df = course_modules_df[course_modules_df["course"] == id].copy()

                    # migrating one course from old to new db
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
                            
                            if not course_modules_filtered_df.empty:
                                logger.debug(f"Inserting {len(course_modules_filtered_df)} course_modules for course {new_course_id}.")

                                course_modules_filtered_df["course"] = new_course_id
                                
                                # changing the module ids
                                course_modules_filtered_df["module"] = course_modules_filtered_df["module"].map(lambda x: new_modules_map.get(old_modules_map.get(x)))

                                # droping ids
                                old_cm_ids = {}
                                course_modules_filtered_df["old_id"] = course_modules_filtered_df["id"]
                                course_modules_filtered_df = course_modules_filtered_df.drop(columns=["id"])

                                # inserting and maping new ids
                                for _, row in course_modules_filtered_df.iterrows():
                                    sql = text(f"""
                                                INSERT INTO {course_modules_table} 
                                                (course, module, instance, section, added, score, indent, visible, visibleold, groupmode, groupingid, completion, completiongradeitemnumber, completionview, completionexpected, availability, showdescription)
                                                VALUES
                                                (:course, :module, :instance, :section, :added, :score, :indent, :visible, :visibleold, :groupmode, :groupingid, :completion, :completiongradeitemnumber, :completionview, :completionexpected, :availability, :showdescription)
                                                """
                                    )
                                    row_cleaned = row.replace({pd.NA: None, '': None}).where(pd.notnull(row), None)
                                    row_dict = row_cleaned.to_dict()
                                    conn.execute(sql, row_dict)

                                    # Map the old cm_id to the new cm_id
                                    old_cm_id = row["old_id"]
                                    new_cm_id = conn.execute(text(f"SELECT id FROM {course_modules_table} ORDER BY id DESC LIMIT 1")).scalar()
                                    module_instance_mapping[old_cm_id] = new_cm_id
                                    #logger.info(f"Mapping: {module_instance_mapping}")

                                    # create course_module context (contextlevel 70)
                                    conn.execute(text(f"""
                                        INSERT INTO {context_table} (contextlevel, instanceid, depth, path)
                                        VALUES (70, :instanceid, 4, NULL)
                                    """), {"instanceid": new_cm_id})

                                    # retrieve new context.id
                                    result = conn.execute(text(f"""
                                        SELECT id FROM {context_table}
                                        WHERE contextlevel = 70 AND instanceid = :instanceid
                                        ORDER BY id DESC LIMIT 1
                                    """), {"instanceid": new_cm_id})
                                    new_cm_context_id = result.scalar()

                                    # update context path
                                    path_cm = f"/1/{context_category_id}/{new_context_id}/{new_cm_context_id}"
                                    conn.execute(text(f"""
                                        UPDATE {context_table}
                                        SET path = :path
                                        WHERE id = :id
                                    """), {"path": path_cm, "id": new_cm_context_id})

                                logger.info(f"{len(course_modules_filtered_df)} course_modules inserted.")

                            if not course_sections_df.empty:
                                logger.debug(f"Course {new_course_id} has {len(course_sections_df)} sections.")
                                
                                section_sequence_map = dict(zip(course_sections_df["section"], course_sections_df["sequence"]))
                                #logger.info(f"section_sequence_map: {section_sequence_map}")
                                course_sections_df["course"] = new_course_id
                                course_sections_df = course_sections_df.drop(columns=["id"])
                                course_sections_df["sequence"] = course_sections_df["sequence"].apply(
                                    lambda seq: transform_sequence(seq, module_instance_mapping)
                                )

                                #logger.debug(f"Updated sequences: {course_sections_df['sequence'].tolist()}")

                                course_sections_df.to_sql(sections_table, conn, if_exists="append", index=False)
                                logger.info(f"{len(course_sections_df)} section(s) inserted for course {new_course_id}.")

                                # get new sections ids
                                result = conn.execute(text(
                                    f"""
                                    SELECT id, section FROM {sections_table}
                                    WHERE course = :course_id
                                    ORDER BY section ASC
                                    """
                                ), {"course_id": new_course_id}).mappings().fetchall()

                                old_to_new_section_ids = {row["section"]: row["id"] for row in result}
                                #logger.debug(f"Section ID mapping: {old_to_new_section_ids}")

                                course_modules_filtered_df["section"] = course_modules_filtered_df["section"].map(old_to_new_section_ids)

                                section_to_sequence_mapping = {}

                                # Iterate over the old_to_new_section_ids and match it with the sequence values
                                for old_section_index, new_section_id in old_to_new_section_ids.items():
                                    sequence_value = section_sequence_map.get(old_section_index, '')
                                    section_to_sequence_mapping[new_section_id] = sequence_value

                                #logger.debug(f"Section ID to Sequence Mapping: {section_to_sequence_mapping}")

                                logger.info("All course_sections updated with correct sequences.")

                            else:
                                logger.warning(f"No course sections found for course ID {id}.")
                            
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
