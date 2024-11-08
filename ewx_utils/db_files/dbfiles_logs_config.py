"""
import sys
import dotenv
dotenv.load_dotenv()
ewx_base_path = os.getenv("EWX_BASE_PATH")
sys.path.append(ewx_base_path)
"""
import os
import logging
import ewx_utils.ewx_config as ewx_config
from logging.handlers import RotatingFileHandler

# Global variable to hold the logger instance
dbfiles_logger_instance = None

def dbfiles_logger(log_path = ewx_config.ewx_log_path):
    global dbfiles_logger_instance
    try:
        # Check if the logger instance already exists
        if dbfiles_logger_instance is None:
            # Creating a custom logger
            dbfiles_logger_instance = logging.getLogger(__name__)

            # Creating a file handler
            file_handler = RotatingFileHandler(
                filename = os.path.join(log_path, "dbfiles_logs.log"),maxBytes=1024, backupCount=3
            )
            # Setting level for the file handler to DEBUG to capture all levels
            file_handler.setLevel(logging.DEBUG)

            # Creating a formatter and adding it to the file handler
            file_formatter = logging.Formatter(
                "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
            )
            file_handler.setFormatter(file_formatter)

            # Adding the file handler to the logger
            dbfiles_logger_instance.addHandler(file_handler)

        return dbfiles_logger_instance

    except Exception as e:
        print(f"An error occurred while setting up the dbfiles logger: {e}")
        return None