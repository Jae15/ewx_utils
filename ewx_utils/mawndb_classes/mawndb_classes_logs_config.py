import sys
import logging
sys.path.append("c:/Users/mwangija/data_file/ewx_utils/ewx_utils")
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler

import logging
from logging.handlers import TimedRotatingFileHandler

# Global variable to hold the logger instance
mawndb_logger_instance = None

def mawndb_classes_logger():
    global mawndb_logger_instance
    try:
        # Check if the logger instance already exists
        if mawndb_logger_instance is None:
            # Creating a custom logger
            mawndb_logger_instance = logging.getLogger(__name__)

            # Creating a file handler
            file_handler = TimedRotatingFileHandler(
                filename="mawndb_classes_logs.log", when="midnight", interval=1, backupCount=7
            )
            # Setting level for the file handler to DEBUG to capture all levels
            file_handler.setLevel(logging.DEBUG)

            # Creating a formatter and adding it to the file handler
            file_formatter = logging.Formatter(
                "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
            )
            file_handler.setFormatter(file_formatter)

            # Adding the file handler to the logger
            mawndb_logger_instance.addHandler(file_handler)

        return mawndb_logger_instance

    except Exception as e:
        print(f"An error occurred while setting up the mawndb logger: {e}")
        return None
