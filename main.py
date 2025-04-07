from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
from src.extract import extract
from src.transform import transform
from src.load import load
from src.logging import start
from urllib.parse import quote_plus
from types import SimpleNamespace


load_dotenv()

logger = start()

def get_env_variable(db_nickname):
    return SimpleNamespace(
        user=os.getenv(f"{db_nickname}_DB_USER"),
        password=quote_plus(os.getenv(f"{db_nickname}_DB_PASSWORD")),
        host=os.getenv(f"{db_nickname}_DB_HOST"),
        port=os.getenv(f"{db_nickname}_DB_PORT"),
        name=os.getenv(f"{db_nickname}_DB_NAME"),
        prefix=os.getenv(f"{db_nickname}_DB_PREFIX")
    )

old_db = get_env_variable("OLD")
new_db = get_env_variable("NEW")


# Cria os engines usando os dicionários
old_engine = create_engine(f"mysql+pymysql://{old_db.user}:{old_db.password}@{old_db.host}:{old_db.port}/{old_db.name}?charset=utf8mb4")
new_engine = create_engine(f"mysql+pymysql://{new_db.user}:{new_db.password}@{new_db.host}:{new_db.port}/{new_db.name}?charset=utf8mb4")

def main():
    try:
        logger.debug("------------------------------------------ NEW ETL RUN ------------------------------------------")
        logger.debug("ETL process started...")
        
        with old_engine.connect() as old_conn, new_engine.begin() as new_conn:  # .connect to reading (no transactions and locks, better performance) // .begin to load (starting transaction -> require commit/rollback)
            dataframes = extract(old_conn, old_db.prefix)
            dataframes = transform(dataframes)
            load(dataframes, new_conn, new_db)
        logger.info("ETL process completed successfully!")
    except Exception as e:
        logger.critical(f"ETL process failed: {e}.")

if __name__ == "__main__":
    main()
