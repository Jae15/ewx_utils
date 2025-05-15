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


class Albedo:
    """
    The Albedo class defines the valid hourly default range for albedo measurements
    It is used to validate albedo data retrieved from mawndb.
    
    Albedo is the fraction of solar radiation reflected by a surface, ranging from 0 to 1.
    """

    # Define valid range for albedo values (0 to 1 inclusive)
    valid_albedo_range = (0, 1)

    def __init__(self, value, record_date=None):
        """
        Initializes the Albedo object.

        Parameters:
        value(float): Albedo value.
        record_date(datetime, optional): The date of the record.
        """
        self.logger = EWXStructuredLogger(log_path=ewx_log_file)
        self.logger.debug("Initializing Albedo object with value: %s, record_date: %s", 
                         value, record_date)
        self.record_date = record_date
        
        if value is not None:
            self.value = round(value, 6)
            self.logger.debug("Rounded value to 6 decimal places: %s", self.value)
        else:
            self.value = None
            self.logger.debug("Value is None")

    def is_valid(self):
        """
        Checks if the albedo value is within the valid range (0 to 1 inclusive).

        Returns:
        bool: True if the albedo value is within the valid range, False otherwise.
        """
        self.logger.debug("Validating albedo value: %s", self.value)

        # Check if value is None
        if self.value is None:
            self.logger.debug("Albedo value is None, returning False.")
            return False
            
        is_valid = self.valid_albedo_range[0] <= self.value <= self.valid_albedo_range[1]
        self.logger.debug("Albedo value: %s is valid: %s (range: %s)", 
                         self.value, is_valid, self.valid_albedo_range)
        return is_valid