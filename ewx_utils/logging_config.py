import logging
import sys
from logging.handlers import RotatingFileHandler

logger = logging.getLogger('ewx_logger')
logger.setLevel(logging.DEBUG)

# Create file handler which logs debug messages
file_handler = logging.FileHandler(filename = 'log_file')
file_handler.setLevel(logging.DEBUG)
file_handler = logging.handlers.RotatingFileHandler('mainlog_file.log', maxBytes = 1000000, backupCount = 5)

# Create console handler with a higher log level
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.ERROR)

# Create formatter and add it to the handlers
formatter = logging.Formatter("%(asctime)s - '%(levelname)s - %(name)s - %(message)s")
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)

# 'application' code
logger.debug('debug message')
logger.info('info message')
logger.warning('warn message')
logger.error('error message')
logger.critical('critical message')
