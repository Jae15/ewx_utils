import os
import sys
from dotenv import load_dotenv
load_dotenv()
ewx_base_path = os.getenv("EWX_BASE_PATH")
sys.path.append(ewx_base_path)
from datetime import datetime
from ewx_utils.ewx_config import ewx_log_file
from ewx_utils.logs.ewx_utils_logs_config import ewx_utils_logger

class Humidity:
    """
    The humidity class below defines the valid hourly default monthly ranges.
    It also specifies the units of measurement and their respective conversions as stored in mawndb.
    """
   
    # Valid range for relative humidity measurements
    valid_relh_hourly_default = (0, 100)
    RELH_CAP = 105

    def __init__(self, relh, units, record_date=None):
        """
        Initializes the Humidity object.

        Parameters:
        relh (float): Relative humidity value.
        units (str): The unit of measurement ('PCT' or 'DEC').
        record_date (datetime, optional): The date of the record.
        """
        self.logger = ewx_utils_logger(log_path = ewx_log_file)
        self.logger.debug("Initializing Humidity object with relh: %s, units: %s, record_date: %s",
                          relh, units, record_date)
       
        self.record_date = record_date
        unitsU = units.upper()
        if relh is None:
            self.relhPCT = None
            self.relhDEC = None

        if unitsU == "PCT":
            self.relhPCT = float(relh)
            self.relhDEC = float(relh) / 100
        elif unitsU == "DEC":
            self.relhPCT = float(relh) * 100
            self.relhDEC = float(relh)
        else:
            raise ValueError(f"Invalid units: {units}")

    def is_valid(self):
        """
        Checks if the relative humidity value is within the valid range.
       
        Returns:
        bool: True if the relative humidity value is within the valid range, False otherwise.
        """
        self.logger.debug("Validating relhPCT value: %s", self.relhPCT)

        if self.valid_relh_hourly_default[0] <= self.relhPCT <= self.valid_relh_hourly_default[1]:
            self.logger.debug("relhPCT value: %s is within the valid range: %s",
                              self.relhPCT, self.valid_relh_hourly_default)
            return True
        else:
            self.logger.debug("relhPCT value: %s is outside the valid range: %s",
                              self.relhPCT, self.valid_relh_hourly_default)
            return False
    
    def set_src(self, src):
        """
        Sets the source of the humidity data.

        Parameters:
        src(str): 
        """
        self.src = src
        self.logger.debug("Source set to: %s src")

    def is_in_range(self):
        """
        Checks if the relative humidity value is in the range of the valid upper limit and the RELH_CAP.

        Returns:
        tuple: (upper limit value, 'RELH_CAP') if in range, (None, None) otherwise.
        """
        # Checking if the humidity value is below the minimum threshold
        if self.relhPCT < 0:
            self.logger.debug("relhPCT value: %s is below 0, returning (0, 'RELH_CAP')", self.relhPCT)
            return (None, "EMPTY") # Cap relh values at 0%, set the values below 0 to None and source to "EMPTY"

        # Checking if the humidity value is within the capped range
        elif 100 < self.relhPCT <= self.RELH_CAP:
            self.logger.debug("relhPCT value: %s is within the range, returning (100, 'RELH_CAP')",
                            self.relhPCT, self.RELH_CAP)
            return (100, "RELH_CAP") # Cap relh values above 100 but below 105 to 100 and set source to "RELH_CAP"

        # Checking if the humidity value is above the maximum threshold
        elif self.relhPCT > self.RELH_CAP:
            self.logger.debug("relhPCT value: %s is above RELH_CAP, returning None", self.relhPCT)
            return (None, "OOR") # Return None for values above RELH_CAP and the source as "OOR"

        # Return the actual value for valid humidity below 100
        else: self.logger.debug("relhPCT value: %s is below 100, returning (%s, 'MAWN')", self.relhPCT)
        return (self.relhPCT, "MAWN") # Return the relh values that are within the valid range and setting their source to "MAWN"

   