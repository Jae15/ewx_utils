import os
import sys
from dotenv import load_dotenv
load_dotenv()
ewx_base_path = os.getenv("EWX_BASE_PATH")
sys.path.append(ewx_base_path)
from datetime import datetime
from ewx_utils.ewx_config import ewx_log_file
from ewx_utils.logs.ewx_utils_logs_config import ewx_utils_logger


class ShortwaveRadiation:
    """
    The ShortwaveRadiation class defines the valid hourly default ranges for shortwave radiation components
    It is used to validate shortwave radiation data retrieved from mawndb.
    """

    def __init__(self, variable_name, value, record_date=None, summation=None):
        """
        Initializes the ShortwaveRadiation object.

        Parameters:
        variable_name(str): Name of the shortwave radiation variable.
        value(float): Value of the shortwave radiation variable.
        record_date(datetime, optional): The date of the record.
        summation(str, optional): Type of summation ("incoming", "outgoing", or "net").
        """
        self.logger = ewx_utils_logger(log_path=ewx_log_file)
        self.logger.debug("Initializing ShortwaveRadiation object with %s: %s, record_date: %s", 
                         variable_name, value, record_date)
        self.record_date = record_date
        self.variable_name = variable_name
        self.summation = summation
        
        if value is not None:
            self.value = round(value, 6)
            self.logger.debug("Rounded value to 6 decimal places: %s", self.value)
        else:
            self.value = None
            self.logger.debug("Value is None")

    def get_valid_range(self):
        """
        Determines the valid range based on variable name or summation type.
        
        Returns:
        tuple: (min_value, max_value) representing the valid range
        """
        self.logger.debug("Getting valid range for %s with summation type %s", 
                         self.variable_name, self.summation)
        
        # Check for incoming shortwave radiation
        if (self.summation == "incoming" or 
            (self.variable_name and self.variable_name.startswith("swin"))):
            self.logger.debug("Identified as incoming shortwave radiation")
            return (-1, 1500)
            
        # Check for outgoing shortwave radiation
        elif (self.summation == "outgoing" or 
              (self.variable_name and self.variable_name.startswith("swout"))):
            self.logger.debug("Identified as outgoing shortwave radiation")
            return (-1, 1500)
            
        # Check for net shortwave radiation
        elif (self.summation == "net" or 
              (self.variable_name and self.variable_name.startswith("swnet"))):
            self.logger.debug("Identified as net shortwave radiation")
            return (-2, 500)
            
        # Default case if no conditions match
        else:
            self.logger.warning("Unknown shortwave radiation type for %s", self.variable_name)
            return None

    def is_valid(self):
        """
        Checks if the shortwave radiation value is within the valid range for its variable type.

        Returns:
        bool: True if the value is within the valid range, False otherwise.
        """
        self.logger.debug("Validating %s value: %s", self.variable_name, self.value)

        # Check if value is None
        if self.value is None:
            self.logger.debug("%s value is None, returning False.", self.variable_name)
            return False
            
        # Get valid range for this variable
        valid_range = self.get_valid_range()
        
        if valid_range is None:
            self.logger.debug("Could not determine valid range for %s, returning False.", self.variable_name)
            return False
            
        is_valid = valid_range[0] <= self.value <= valid_range[1]
        self.logger.debug("%s value: %s is valid: %s (range: %s)", 
                         self.variable_name, self.value, is_valid, valid_range)
        return is_valid