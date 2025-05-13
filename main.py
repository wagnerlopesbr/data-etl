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
        # close any previous connection pool to start clean
        old_engine.dispose()
        new_engine.dispose()
        logger.debug("ETL process started...")

        # Use connection context managers
        # - old_engine.connect(): read-only, better for performance (no locking/transactions)
        # - new_engine.begin(): transactional, used for write operations (auto commit/rollback)
        
        with old_engine.connect() as old_conn, new_engine.begin() as new_conn:
            dataframes = extract(old_conn, new_conn, old_db.prefix, new_db.prefix)
            dataframes = transform(dataframes)
            load(dataframes, new_conn, new_db)
        logger.info("ETL process completed successfully!")
    except Exception as e:
        logger.critical(f"ETL process failed: {e}.")
    finally:
        # dispose all connections after the process ends
        old_engine.dispose()
        new_engine.dispose()
        logger.debug("Disposed all database connections after ETL run.")

if __name__ == "__main__":
    main()
