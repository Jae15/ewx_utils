import logging
import sys
from logging.handlers import RotatingFileHandler

# Global variable to hold the logger instance
dbfiles_logger_instance = None

def dbfiles_logger():
    global dbfiles_logger_instance
    try:
        # Check if the logger instance already exists
        if dbfiles_logger_instance is None:
            # Creating a custom logger
            dbfiles_logger_instance = logging.getLogger(__name__)

            # Creating a file handler
            file_handler = RotatingFileHandler(
                filename="dbfiles_logs.log", maxBytes=1024, backupCount=3
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