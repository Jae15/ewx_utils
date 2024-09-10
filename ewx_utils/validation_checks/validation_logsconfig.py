import logging
from logging.handlers import RotatingFileHandler
from logging.handlers import TimedRotatingFileHandler


def validations_logger():
    # Creating a custom logger
    logger = logging.getLogger(__name__)

 # Creating handlers
    console_handler = logging.StreamHandler()
    file_handler = TimedRotatingFileHandler(
        filename="dryrunlogs.log", when="midnight", interval=1, backupCount=7
    )

    # time rotating file handler eg 1 per day, can increase max size,

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
