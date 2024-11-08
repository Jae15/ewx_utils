import sys
import logging
import dotenv
dotenv.load_dotenv()
import os
ewx_path = os.getenv("EWX_PATH")
sys.path.append(ewx_path)
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from logging.handlers import TimedRotatingFileHandler

# Global variable to hold the logger instance
mawndb_logger_instance = None
def mawndb_classes_logger(log_path = "../log"):
    global mawndb_logger_instance
    try:
        # Check if the logger instance already exists
        if mawndb_logger_instance is None:
            # Creating a custom logger
            mawndb_logger_instance = logging.getLogger(__name__)

            # Creating a file handler
            file_handler = TimedRotatingFileHandler(
                filename= os.path.append(log_path, "mawndb_classes_logs.log"), when="midnight", interval=1, backupCount=7
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
