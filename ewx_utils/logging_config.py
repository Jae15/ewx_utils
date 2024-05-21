import logging

logger = logging.getLogger('simple_example')
logger.setLevel(logging.DEBUG)

# Create file handler which logs debug messages
file_handler = logging.FileHandler('log_file')
file_handler.setLvel(logging.DEBUG)

# Create console handler with a higher log level
console_handler = logging.StreamHandler
console_handler.setLevel(logging.ERROR)

# Create formatter and add it to the handlers
formatter = logging.Formatter('%(levelname)s - %(asctime)s - %(name)s - %(message)s')
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