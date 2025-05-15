import os
import sys
import logging
from logging.handlers import TimedRotatingFileHandler
import structlog

"""
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

"""

logger_instance_unstructured = None

def ewx_unstructured_logger(log_path="../log"):
    """
    Returns a standard unstructured logger (plain text).
    """
    global logger_instance_unstructured
    try:
        if logger_instance_unstructured is None:
            os.makedirs(log_path, exist_ok=True)
            logger = logging.getLogger(__name__)
            logger.setLevel(logging.DEBUG)

            file_handler = TimedRotatingFileHandler(
                filename=os.path.join(log_path, "ewx_logs.log"),
                when="midnight",
                interval=1,
                backupCount=7,
                encoding="utf-8"
            )
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
            file_handler.setFormatter(formatter)

            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)

            logger.addHandler(file_handler)
            logger.addHandler(console_handler)

            logger_instance_unstructured = logger

        return logger_instance_unstructured

    except Exception as e:
        print(f"An error occurred while setting up the logger: {e}")
        return None

class EWXStructuredLogger:
    """
    Structured logger using structlog.
    - Logs all levels to file in JSON format.
    - Logs only ERROR and CRITICAL to console.
    - Supports direct method access via __getattr__.
    """

    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(EWXStructuredLogger, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, log_path="../log"):
        if self._initialized:
            return

        self.log_path = log_path
        self.logger = None
        self._setup_logger()
        self._initialized = True

    def _setup_logger(self):
        os.makedirs(self.log_path, exist_ok=True)

        # Create file handler (accepts all logs)
        file_handler = TimedRotatingFileHandler(
            filename=os.path.join(self.log_path, "ewx_logs.log"),
            when="midnight",
            interval=1,
            backupCount=7,
            encoding="utf-8"
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter('%(message)s'))

        # Create console handler (only ERROR and above)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.ERROR)
        console_handler.setFormatter(logging.Formatter('%(message)s'))

        # Configure base logging (handlers and root level)
        logging.basicConfig(
            level=logging.DEBUG,  # Capture all logs at the root
            handlers=[file_handler, console_handler]
        )

        # Configure structlog
        structlog.configure(
            processors=[
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.stdlib.add_log_level,
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.JSONRenderer(),
            ],
            context_class=dict,
            wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG),
            logger_factory=structlog.stdlib.LoggerFactory(),
            cache_logger_on_first_use=True,
        )

        self.logger = structlog.get_logger()

    def get_logger(self):
        return self.logger

    def __getattr__(self, name):
        """
        Delegate logging methods like .info(), .error(), etc., to the structlog logger.
        """
        return getattr(self.logger, name)


