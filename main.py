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
        """
        csv_1 = 'src/utils/teste_1.csv'
        id_list_1 = extract_old_course_ids_from_csv(csv_1)
        csv_2 = 'src/utils/teste_2.csv'
        id_list_2 = extract_old_course_ids_from_csv(csv_2)
        """
        csv = 'src/utils/teste_3.csv'
        id_list = extract_old_course_ids_from_csv(csv)
        print(f"id_list: {id_list}")

        with old_engine.connect() as old_conn:
            old_dataframes = extract(old_conn, old_db.prefix, "old")
        
        with new_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as new_conn:
            new_dataframes = extract(new_conn, new_db.prefix, "new")

        dataframes = {**old_dataframes, **new_dataframes}

        dataframes = transform(dataframes)
        """
        image_texts_1 = downloading(dataframes, id_list_1)
        image_texts_2 = downloading(dataframes, id_list_2)
        image_texts = {**image_texts_1, **image_texts_2}
        """
        image_texts = downloading(dataframes, id_list)
        with new_engine.begin() as write_conn:
            """
                categories by int id (NEW DB CATEGORY ID):
                    1 = 'QSMS'
                    4 = 'Sistemas de Gestão'
                    6 = 'Administração'
                    10 = 'Treinamentos Internos'
                    11 = 'Rodolfo'
                    12 = 'R. Velasquez Medeiros'
                    13 = 'DICA Tecnologia Soluções Inteligentes'
                    14 = 'Cursos Personalizados'
                    16 = 'MODELOS'
                    17 = 'Turmas Presenciais'
                    18 = 'Galáxia Marítima'
                    19 = 'Galáxia Navegação'
                course_language by string (affects 'feedback_item'):
                    'en'
                    'ptbr'
                customcert_template (cc_template_to_use) by string (affects 'customcert_templates', 'customcert_pages' and 'customcert_elements'):
                    'default_ptbr'
                    'default_en'
                    'antigo_vertical'
                    'dica'
                    'galaxia_maritima'
                    'galaxia_navegacao'
                    'rvelasquez'
            """
            load(dataframes, write_conn, new_db, id_list, image_texts, 19, "antigo_vertical", "en")
            #load(dataframes, write_conn, new_db, id_list_2, image_texts, 17, "dica", "ptbr")
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
