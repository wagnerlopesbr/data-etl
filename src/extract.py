import pandas as pd


def extract(engine, DB_PREFIX: str) -> pd.DataFrame:
    print(f"🔎 Starting the extraction process...\n\n")
    
    queries = {
        "course": f"SELECT * FROM {DB_PREFIX}_course WHERE format <> 'site'",
        "course_sections": f"SELECT * FROM {DB_PREFIX}_course_sections",
        "course_modules": f"SELECT * FROM {DB_PREFIX}_course_modules",
        "modules": f"SELECT * FROM {DB_PREFIX}_modules",
        "page": f"SELECT * FROM {DB_PREFIX}_page",
        "feedback": f"SELECT * FROM {DB_PREFIX}_feedback",
        "choice": f"SELECT * FROM {DB_PREFIX}_choice",
        "quiz": f"SELECT * FROM {DB_PREFIX}_quiz",
        "url": f"SELECT * FROM {DB_PREFIX}_url",
        "forum": f"SELECT * FROM {DB_PREFIX}_forum",
        "reengagement": f"SELECT * FROM {DB_PREFIX}_reengagement",
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
                                       FROM {DB_PREFIX}_course_modules cm
                                       JOIN {DB_PREFIX}_course c ON c.id = cm.course
                                       JOIN {DB_PREFIX}_course_sections cs ON cs.id = cm.section
                                       JOIN {DB_PREFIX}_modules m ON m.id = cm.module
                                    """
    }

    specific_joins = ["page", "feedback", "quiz", "url", "forum", "reengagement"]

    for type in specific_joins:
        queries[f"course_{type}_instances"] = f"""
            SELECT
                c.id AS course_id,
                c.fullname AS course_name,
                cm.id AS course_module_id,
                cm.module AS course_module_type_id,
                m.name AS course_module_type,
                cm.instance AS course_module_instance_id,
                t.*
            FROM {DB_PREFIX}_course_modules cm
            JOIN {DB_PREFIX}_course c ON c.id = cm.course
            JOIN {DB_PREFIX}_course_sections cs ON cs.id = cm.section
            JOIN {DB_PREFIX}_modules m ON m.id = cm.module
            JOIN {DB_PREFIX}_{type} t ON t.id = cm.instance AND m.name = '{type}'
        """

    dataframes = {}
    with engine.connect() as connection:
        for table, query in queries.items():
            df = pd.read_sql(query, connection)
            print(f"✅ Extracted {len(df)} rows from {table.upper()}\n")
            dataframes[table] = df
    print(f"🔎 End of extraction process\n\n")
    return dataframes
