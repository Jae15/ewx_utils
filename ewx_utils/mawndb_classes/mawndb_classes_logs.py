import logging
import sys
from logging.handlers import RotatingFileHandler


def mawndb_classes_logger():
    # Creating  a custom logger
    logger = logging.getLogger(__name__)

    # Creating handlers
    console_handler = logging.StreamHandler()
    file_handler = RotatingFileHandler(
        filename="validation_logs.log", maxBytes=1024, backupCount=3
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
