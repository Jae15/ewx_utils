import os
import sys
from dotenv import load_dotenv
load_dotenv()
ewx_base_path = os.getenv("EWX_BASE_PATH")
sys.path.append(ewx_base_path)
from datetime import datetime
from ewx_utils.ewx_config import ewx_log_file
from ewx_utils.logs.ewx_utils_logs_config import ewx_utils_logger

class SoilMoisture:
    """
    The SoilMoisture class defines the valid hourly default ranges for soil moisture
    """
    valid_mstr_hourly_default = (0, 1)

    def __init__(self, mstr, record_date=None):
        """
        Initializes the SoilMoisture object.

        Parameters:
        mstr(float): Soil moisture value.
        record_date(datetime, optional): The date of the record.
        
        """
        self.logger = ewx_utils_logger(log_path = ewx_log_file)
        self.logger.debug("Initializing SoilMoisture object with mstr: %s, record_date: %s", mstr, record_date)

        self.record_date = record_date
        self.mstr = mstr

        # Set mstr to None if it is negative
        if mstr < 0:
            self.mstr = None
        else:
            self.mstr = mstr

    def is_valid(self):
        """
        Checks if the soil moisture value is within the valid hourly default range.

        Returns:
        bool: True if the soil moisture value is within the valid range, False otherwise.
        """
        self.logger.debug("Validating soil moisture value: %s", self.mstr)

        # Check if mstr is None
        if self.mstr is None:
            self.logger.debug("Soil moisture value is None, returning False.")
            return False

        is_valid = self.valid_mstr_hourly_default[0] <= self.mstr <= self.valid_mstr_hourly_default[1]
        self.logger.debug("Soil moisture value: %s is valid: %s", self.mstr, is_valid)

        return is_valid


