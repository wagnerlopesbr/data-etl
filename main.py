from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
from src.extract import extract, extract_old_course_ids_from_csv
from src.transform import transform
from src.load import load, downloading
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

# Connection pool configuration to ensure stability and prevent idle disconnects
engine_options = {
    "pool_pre_ping": True,   # Check connection liveness before each use
    "pool_recycle": 3600,    # Recycle connections after 1 hour to avoid timeouts
    "pool_size": 5,          # Base number of persistent connections in the pool
    "max_overflow": 10       # Additional temporary connections allowed during high demand
}

# Create engines with connection stability settings
old_engine = create_engine(
    f"mysql+pymysql://{old_db.user}:{old_db.password}@{old_db.host}:{old_db.port}/{old_db.name}?charset=utf8mb4",
    **engine_options
)
new_engine = create_engine(
    f"mysql+pymysql://{new_db.user}:{new_db.password}@{new_db.host}:{new_db.port}/{new_db.name}?charset=utf8mb4",
    **engine_options
)

def main():
    try:
        logger.debug("------------------------------------------ NEW ETL RUN ------------------------------------------")
        logger.debug("ETL process started...")

        # close any previous connection pool to start clean
        old_engine.dispose()
        new_engine.dispose()

        # Use connection context managers
        # - old_engine.connect(): read-only, better for performance (no locking/transactions)
        # - new_engine.begin(): transactional, used for write operations (auto commit/rollback)

        # tracking data from specific .csv
        csv = 'src/utils/cursos_teste.csv'
        ids_to_migrate = extract_old_course_ids_from_csv(10, csv)
                
        with old_engine.connect() as old_conn:
            old_dataframes = extract(old_conn, old_db.prefix, "old")
        
        with new_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as new_conn:
            new_dataframes = extract(new_conn, new_db.prefix, "new")

        dataframes = {**old_dataframes, **new_dataframes}

        dataframes = transform(dataframes)
        
        image_texts = downloading(dataframes, ids_to_migrate)
        with new_engine.begin() as write_conn:
            load(dataframes, write_conn, new_db, ids_to_migrate, image_texts)
        logger.info("ETL process completed successfully!")
    except Exception as e:
        logger.critical(f"ETL process failed: {e}.")
    finally:
        # close any previous connection pool to start clean
        old_engine.dispose()
        new_engine.dispose()
        logger.debug("Disposed all database connections after ETL run.")

if __name__ == "__main__":
    main()
