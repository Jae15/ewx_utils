import sys
import logging
from datetime import datetime
sys.path.append("c:/Users/mwangija/data_file/ewx_utils/ewx_utils")
from mawndb_classes.mawndb_classes_logs_config import mawndb_classes_logger

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
        self.logger = mawndb_classes_logger()
        self.logger.debug("Initializing Humidity object with relh: %s, units: %s, record_date: %s",
                          relh, units, record_date)
       
        self.record_date = record_date
        if relh is None:
            relh = -9999
        unitsU = units.upper()
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
            return (0, 'RELH_CAP')  # Cap at 0%

        # Checking if the humidity value is within the capped range
        if 100 <= self.relhPCT <= self.RELH_CAP:
            self.logger.debug("relhPCT value: %s is within the range, returning (100, 'RELH_CAP')",
                            self.relhPCT, self.RELH_CAP)
            return (100, 'RELH_CAP')

        # Checking if the humidity value is above the maximum threshold
        elif self.relhPCT > self.RELH_CAP:
            self.logger.debug("relhPCT value: %s is above RELH_CAP, returning None", self.relhPCT)
            return None  # Return None for values above 105

        # Return the actual value for valid humidity below 100
        self.logger.debug("relhPCT value: %s is below 100, returning (%s, 'RELH')", self.relhPCT)
        return (self.relhPCT, 'RELH')
    
 
