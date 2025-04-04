import logging
import os


# setting log_level to "debug" because its the lowest level in the hierarchy, allowing all the higher-level messages to be logged as well
def start(log_level="debug", log_dir="src/logs", name="etl_logger"):
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, "etl.log")
    logger = logging.getLogger(name)

    # clean HANDLERS
    if logger.hasHandlers():
        logger.handlers.clear()

    # convert log_level string to lower case ("THIS" to "this")
    level = log_level.lower()
    
    level_map = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL
    }

    selected_level = level_map.get(level, logging.DEBUG)

    logger.setLevel(logging.DEBUG)  # get all levels internally

    # Formatter
    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )

    # File handler 
    file_handler = logging.FileHandler(log_path, mode="a", encoding="utf-8")  # mode="a" to append new logs
    file_handler.setLevel(selected_level)  # filtering which method
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
