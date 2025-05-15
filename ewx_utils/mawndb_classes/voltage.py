import os
import sys
from dotenv import load_dotenv
load_dotenv()
ewx_base_path = os.getenv("EWX_BASE_PATH")
sys.path.append(ewx_base_path)
from datetime import datetime
from ewx_utils.ewx_config import ewx_log_file
from ewx_utils.logs.ewx_utils_logs_config import ewx_unstructured_logger
from ewx_utils.logs.ewx_utils_logs_config import EWXStructuredLogger

class Voltage:
    """
    The Voltage class defines the valid hourly default range for station battery voltage values.

    It is used to validate data retrieved from the mawndb database in dbh11 server.
    """
    valid_volt_hourly_default = (0, 20)

    def __init__(self, volt, record_date=None):
        """
        Initializes the Voltage class.

        Parameters:
        volt(float): Voltage value
        record_date(datetime, optional): The date of the record.
        """
        self.logger = EWXStructuredLogger(log_path = ewx_log_file)
        self.logger.debug("Initializing Voltage object with volt: %s, record_date: %s", volt, record_date)
        
        self.record_date = record_date
        self.volt = volt

    def set_src(self, src):
        """
        Initializes the source of stations' battery voltage data.

        Parameters:
        src(str): Source of the data
        """

        self.src = src
        self.logger.debug("Source set to: %s, src")

    def is_valid(self):
        """
        Checks if the voltage value is within the valid hourly default range.

        Returns:
        bool: True if the voltage value is within the valid range, False otherwise.
        """
        self.logger.debug("Validating voltage value: %s", self.volt)

        # Check if volt is None
        if self.volt is None:
            self.logger.debug("Voltage value is None, returning False.")
            return False

        is_valid = self.valid_volt_hourly_default[0] <= self.volt <= self.valid_volt_hourly_default[1]
        self.logger.debug("Voltage value: %s is valid: %s", self.volt, is_valid)
        return is_valid
