import sys
import logging
sys.path.append("c:/Users/mwangija/data_file/ewx_utils/ewx_utils")
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler

def mawndb_classes_logger():
    # Creating a custom logger
    logger = logging.getLogger(__name__)

    # Creatingg handlers
    console_handler = logging.StreamHandler()
    file_handler = RotatingFileHandler(
        filename = "mawndb_classes_logs.logs", maxBytes=1024, backupCount=5
    )

    # Setting levels for the handlers
    console_handler.setLevel(logging.DEBUG)
    file_handler.setLevel(logging.ERROR)

   # Creating formatters and adding them to log handlers
    console_formatter = logging.Formatter(
       "%(asctime)s - %(levelname)s - %(name)s - %(message)s"

   )
    file_formatter = logging.Formatter(
       "%(asctime)s - %(levelname)s - %(name)s - %(message)s"

   )

    # Set the formatters for the handlers
    console_handler.setFormatter(console_formatter)
    file_handler.setFormatter(file_formatter)

    # Adding handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger
