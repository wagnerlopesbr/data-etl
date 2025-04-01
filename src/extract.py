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
    }

    dataframes = {}
    with engine.connect() as connection:
        for table, query in queries.items():
            df = pd.read_sql(query, connection)
            print(f"✅ Extracted {len(df)} rows from {table.upper()}\n")
            dataframes[table] = df
    print(f"🔎 End of extraction process\n\n")
    return dataframes
