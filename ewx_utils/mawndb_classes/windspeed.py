import sys
import logging
sys.path.append("c:/Users/mwangija/data_file/ewx_utils/ewx_utils")
from mawndb_classes.mawndb_classes_logs_config import mawndb_classes_logger

class WindSpeed: 
    """
    The windspeed class below defines the valid hourly default monthly ranges.
    It also specifies the units of measurement and their respective conversions as stored in mawndb.
    """

    valid_wspd_hourly_default = (0, 99)

    def __init__(self, wspd, units, record_date=None):
        """
        Initializes the Windspeed object.

        Parameters:
        wspd(float): Wind speed value.
        units(str): The unit of measurement ('MPS' or 'MPH').
        record_date(datetime, optional): The date of the record.
    
        """
        self.logger = mawndb_classes_logger()
        self.logger.debug("Initializing Windspeed object with wspd: %s, units: %s, record_date: %s",
                          wspd, units, record_date)
        self.record_date = record_date
        if wspd is None:
            wspd = -9999
            self.logger.debug("Wind speed is None, setting all speed values to None")
            return
        unitsU = units.upper()
        if unitsU == "MPS":  # meters per second
            self.wspdMPS = float(wspd)
            self.wspdMPH = float(wspd) / 1609.344 * 3600
            # 1609 meters per mile
            # 3600 seconds per hour
        elif unitsU == "MPH":  # miles per hour
            self.wspdMPS = float(wspd) * 1609.344 / 3600
            self.wspdMPH = float(wspd)
        else: 
            self.logger.error("Invalid units provided: %s", units)
            raise ValueError("Units must be 'MPS' or 'MPH'")
        
    def set_src(self, src):
        """
        Initializes the source of wind direction data.

        Parameters:
        src(str): Source of the data
        """
        self.src = src
        self.logger.debug("Source set to: %s, src")

    def is_valid(self):
        """
        Checks if the wind speed is within the valid hourly default range.

        Returns:
        bool: True if the wind speed is within the valid range, False otherwise.
        """
        self.logger.debug("Validating wind speed value: %s", self.wspdMPS)
        if self.wspdMPS is None:
            self.logger.debug("Wind speed is None, returning False")
            return False
        is_valid = self.valid_wspd_hourly_default[0] <= self.wspdMPS <= self.valid_wspd_hourly_default[1]
        self.logger.debug("Wind speed value: %s is valid: %s", self.wspdMPS, is_valid)
        return is_valid
       
