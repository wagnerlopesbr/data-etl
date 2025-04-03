from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
from src.extract import extract
from src.transform import transform
from src.load import load
from src.logging import start

load_dotenv()

logger = start()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_PREFIX = os.getenv("DB_PREFIX")

engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

def main():
    try:
        logger.debug("ETL process started...")
        dataframes = extract(engine, DB_PREFIX)
        dataframes = transform(dataframes)
        load(dataframes)
        logger.info("ETL process completed successfully!")
    except Exception as e:
        logger.critical(f"ETL process failed: {e}.")

if __name__ == "__main__":
    main()
