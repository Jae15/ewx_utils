import logging
from logging.handlers import TimedRotatingFileHandler


def dryrun_logger():
    # Creating a custom logger
    logger = logging.getLogger("dryrun_logger")

    # Set the overall logging level for the logger
    logger.setLevel(logging.DEBUG)

    # Creating handlers
    console_handler = logging.StreamHandler()
    file_handler = TimedRotatingFileHandler(
        filename="dryrunlogs.log", when="midnight", interval=1, backupCount=7
    )

    # Setting levels for the handlers
    console_handler.setLevel(logging.DEBUG)
    file_handler.setLevel(logging.ERROR)

    # Creating formatters and adding them to log handlers
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    )
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Adding handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger
