from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
from src.extract import extract
from src.transform import transform_page, transform_choice
from src.load import load

load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_PREFIX = os.getenv("DB_PREFIX")

engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

def main():
    dataframes = extract(engine, DB_PREFIX)

    if "page" in dataframes or "choice" in dataframes:
        print(f"♻️ Starting the transforming process...\n")

        if "page" in dataframes:
            dataframes["page"] = transform_page(dataframes["page"])

        if "choice" in dataframes:
            dataframes["choice"] = transform_choice(dataframes["choice"])

        print(f"♻️ End of transforming process\n")

    load(dataframes)


if __name__ == "__main__":
    main()
