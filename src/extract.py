import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_PREFIX = os.getenv("DB_PREFIX")

engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")


def extract_data():
    # testing the connection
    queries = {
        "courses": f"SELECT * FROM {DB_PREFIX}_course WHERE format <> 'site'",
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
        with connection.begin():
            try:
                for name, query in queries.items():
                    df = pd.read_sql(query, connection)
                    dataframes[name] = df
                    if not df.empty:
                        print(f"✅ DataFrame {name.upper()} extracted successfully with {len(df)} rows.")
                        # testing the access to a specific column
                        if "name" in df.columns:
                            filtered_values = df["name"]
                            if "Política de Assinatura" in filtered_values.values or "Signature Policy" in filtered_values.values:
                                print(f"✅ DataFrame {name.upper()} 'name' column values:\n{filtered_values.to_string(index=False)}")
                                # and specific values like "Política de Assinatura" or "Signature Policy"
                                count_not_matching = filtered_values[~filtered_values.isin(["Política de Assinatura", "Signature Policy"])].count()
                                print(f"🔢 {count_not_matching} rows not matching 'Política de Assinatura' or 'Signature Policy' element names.")
                        print(f"✅ DataFrame {name.upper()} has {len(df.columns)} columns: {df.columns.tolist()}.\n")
                        # print(f"✅ DataFrame {name} extracted successfully with {len(df)} rows.\n{df.head().to_string(index=False)}\n")
                    else:
                        print(f"⚠️ Warning: DataFrame {name.upper()} is empty.\n")
            except Exception as e:
                print(f"Error extracting data: {e}")
                raise

if __name__ == "__main__":
    extract_data()
