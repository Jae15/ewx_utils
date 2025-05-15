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

class NetRadiation:
    """
    The NetRadiation class defines the valid hourly default range for net radiation
    It is used to validate net radiation data retrieved from mawndb.

    """

    valid_nrad_hourly_default = (-1250, 1250)

    def __init__(self, nrad, record_date=None):
        """
        Initializes the NetRadiation object.

        Parameters:
        nrad(float): Net radiation value.
        record_date(datetime, optional): The date of the record.
        
        """
        self.logger = EWXStructuredLogger(log_path = ewx_log_file)
        self.logger.debug("Initializing NetRadiation object with nrad: %s, record_date: %s", nrad, record_date)
        self.record_date = record_date
        self.nrad = nrad

        if self.nrad is not None:
            self.nrad = round(nrad, 6)
        else:
            self.nrad = None


    def is_valid(self):
        """
        Checks if the net radiation value is within the valid hourly default range.

        Returns:
        bool: True if the net radiation value is within the valid range, False otherwise.
        """
        self.logger.debug("Validating net radiation value: %s", self.nrad)

        # Check if nrad is None
        if self.nrad is None:
            self.logger.debug("Net radiation value is None, returning False.")
            return False

        is_valid = self.valid_nrad_hourly_default[0] <= self.nrad <= self.valid_nrad_hourly_default[1]
        self.logger.debug("Net radiation value: %s is valid: %s", self.nrad, is_valid)
        return is_valid