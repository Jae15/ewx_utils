import logging
import sys
from logging.handlers import RotatingFileHandler

def ewx_utils_logger():
    # Creating  a custom logger
    logger = logging.getLogger(__name__)
    #print(__name__)

    # Creating handlers
    console_handler = logging.StreamHandler()
    file_handler = logging.FileHandler(filename = 'ewxutils_logfile.log')

    # Setting levels for the handlers
    console_handler.setLevel(logging.DEBUG)
    file_handler.setLevel(logging.ERROR)

    # Creating formatters and adding them to log handlers
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')

    # Set the formatters for the handlers
    console_handler.setFormatter(console_formatter)
    file_handler.setFormatter(file_formatter)

    # Adding handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    # 'application' code
    logger.debug('debug message')
    logger.error('error message')

    return logger

    # 'application' code
    #logger.info('info message')
    #logger.warning('warning message')
    #logger.error('error message')
    #logger.critical('critical message')