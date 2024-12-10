import logging
import os
from logging.handlers import TimedRotatingFileHandler


# Global variable to hold the logger instance
logger_instance = None

def ewx_utils_logger(log_path = "../log"):
    global logger_instance
    try:
        # Check if the logger instance already exists
        if logger_instance is None:
            # Creating a custom logger
            logger_instance = logging.getLogger(__name__)
            
            # Creating a file handler
            file_handler = TimedRotatingFileHandler(
                filename= os.path.join(log_path, "ewx_logs.log"), when="midnight", interval=1, backupCount=7
            )

            # Setting level for the file handler to DEBUG to capture all levels
            file_handler.setLevel(logging.DEBUG)

            # Creating a formatter and adding it to the file handler
            file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
            file_handler.setFormatter(file_formatter)

            # Adding the file handler to the logger
            logger_instance.addHandler(file_handler)

        return logger_instance

    except Exception as e:
        print(f"An error occurred while setting up the logger: {e}")
        return None