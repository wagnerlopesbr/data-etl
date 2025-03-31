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
        "course1": f"SELECT id, fullname FROM {DB_PREFIX}_course WHERE id = 24",
        "course2": f"SELECT id, fullname FROM {DB_PREFIX}_course WHERE id = 100",
        "course3": f"SELECT id, fullname FROM {DB_PREFIX}_course WHERE id = 405",
    }

    dataframes = {}
    with engine.connect() as connection:
        with connection.begin():
            try:
                for name, query in queries.items():
                    df = pd.read_sql(query, connection)
                    dataframes[name] = df
                    if not df.empty:
                        print(f"✅ DataFrame {name} extracted successfully.\n{df.to_string(index=False)}\n")
                    else:
                        print(f"⚠️ Warning: DataFrame {name} is empty.")
            except Exception as e:
                print(f"Error extracting data: {e}")
                raise

if __name__ == "__main__":
    extract_data()
