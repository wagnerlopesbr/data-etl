import pandas as pd
from src.logging import start


logger = start()


def extract(old_conn, new_conn, old_db_prefix, new_db_prefix):
    logger.debug(f"Starting the extraction process...")
    
    old_queries = {
        "context_course": f"SELECT * FROM {old_db_prefix}_context WHERE contextlevel = 50",
        "context_category": f"SELECT * FROM {old_db_prefix}_context WHERE contextlevel = 40",
        "course_categories": f"SELECT * FROM {old_db_prefix}_course_categories",
        "course": f"SELECT * FROM {old_db_prefix}_course WHERE format <> 'site'",
        "course_sections": f"SELECT * FROM {old_db_prefix}_course_sections ORDER BY section ASC",
        "course_modules": f"SELECT * FROM {old_db_prefix}_course_modules",
        "modules": f"SELECT * FROM {old_db_prefix}_modules",
        "page": f"SELECT * FROM {old_db_prefix}_page",
        "feedback": f"SELECT * FROM {old_db_prefix}_feedback",
        "choice": f"SELECT * FROM {old_db_prefix}_choice ORDER BY id ASC",
        "choice_options": f"SELECT * FROM {old_db_prefix}_choice_options ORDER BY choiceid ASC",
        "quiz": f"SELECT * FROM {old_db_prefix}_quiz",
        "url": f"SELECT * FROM {old_db_prefix}_url",
        "enrol": f"SELECT * FROM {old_db_prefix}_enrol ORDER BY courseid ASC",
        "forum": f"SELECT * FROM {old_db_prefix}_forum",
        "label": f"SELECT * FROM {old_db_prefix}_label",
        "resource": f"SELECT * FROM {old_db_prefix}_resource",
        "reengagement": f"SELECT * FROM {old_db_prefix}_reengagement",
        #"hvp": f"SELECT * FROM {old_db_prefix}_hvp ORDER BY id ASC",
        "course_format_options": f"SELECT * FROM {old_db_prefix}_course_format_options ORDER BY sectionid ASC",
        "course_modules_sections": f"""
                                       SELECT
                                       c.id AS course_id,
                                       c.fullname AS course_name,
                                       cs.id AS course_section_id,
                                       cs.name AS course_section_name,
                                       cs.sequence AS course_section_sequence,
                                       cm.id AS course_module_id,
                                       cm.module AS course_module_type_id,
                                       m.name AS course_module_type,
                                       cm.instance AS course_module_instance_id
                                       FROM {old_db_prefix}_course_modules cm
                                       JOIN {old_db_prefix}_course c ON c.id = cm.course
                                       JOIN {old_db_prefix}_course_sections cs ON cs.id = cm.section
                                       JOIN {old_db_prefix}_modules m ON m.id = cm.module
                                    """
    }

    new_queries = {
        "customcert_templates_ptbr": f"SELECT * FROM {new_db_prefix}_customcert_templates WHERE id = 2 LIMIT 1",
        "customcert_pages_ptbr": f"SELECT * FROM {new_db_prefix}_customcert_pages WHERE templateid = 2 ORDER BY sequence ASC",
        "customcert_elements_ptbr": f"SELECT * FROM {new_db_prefix}_customcert_elements WHERE pageid IN (3, 10) ORDER BY id ASC",
        "customcert_templates_en": f"SELECT * FROM {new_db_prefix}_customcert_templates WHERE id = 15 LIMIT 1",
        "customcert_pages_en": f"SELECT * FROM {new_db_prefix}_customcert_pages WHERE templateid = 15 ORDER BY sequence ASC",
        "customcert_elements_en": f"SELECT * FROM {new_db_prefix}_customcert_elements WHERE pageid IN (222, 223) ORDER BY id ASC"
    }

    specific_joins = ["page", "feedback", "quiz", "url", "forum", "reengagement"]

    for type in specific_joins:
        old_queries[f"course_{type}_instances"] = f"""
            SELECT
                c.id AS course_id,
                c.fullname AS course_name,
                cm.id AS course_module_id,
                cm.module AS course_module_type_id,
                m.name AS course_module_type,
                cm.instance AS course_module_instance_id,
                t.*
            FROM {old_db_prefix}_course_modules cm
            JOIN {old_db_prefix}_course c ON c.id = cm.course
            JOIN {old_db_prefix}_course_sections cs ON cs.id = cm.section
            JOIN {old_db_prefix}_modules m ON m.id = cm.module
            JOIN {old_db_prefix}_{type} t ON t.id = cm.instance AND m.name = '{type}'
        """

    dataframes = {}
    for table, query in old_queries.items():
        try:
            logger.debug(f"Running query for '{table.upper()}'...")
            df = pd.read_sql(query, old_conn)
            logger.info(f"Extracted {len(df)} rows from {table.upper()} (OLD).")
            dataframes[table] = df
        except Exception as e:
            logger.error(f"Error extracting {table.upper()} from OLD DB: {e}.")
    
    """
    for table, query in new_queries.items():
        try:
            logger.debug(f"Running query for '{table.upper()}'...")
            df = pd.read_sql(query, new_conn)
            logger.info(f"Extracted {len(df)} rows from {table.upper()} (NEW).")
            dataframes[table] = df
        except Exception as e:
            logger.error(f"Error extracting {table.upper()} from NEW DB: {e}.")
    """
            
    logger.info(f"End of extraction process.")
    return dataframes
