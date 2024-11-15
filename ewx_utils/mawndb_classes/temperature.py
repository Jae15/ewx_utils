import os
import sys
from dotenv import load_dotenv
load_dotenv()
ewx_base_path = os.getenv("EWX_BASE_PATH")
sys.path.append(ewx_base_path)
from datetime import datetime
from ewx_utils.ewx_config import ewx_log_file
from ewx_utils.logs.ewx_utils_logs_config import ewx_utils_logger

class Temperature:
    """
    The Temperature class defines the valid hourly default monthly ranges.
    It also specifies the units of measurement and their respective conversions as stored in mawndb.
    """

    valid_hourly_atmp = {
        "jan": (-39, 25),
        "feb": (-39, 24),
        "mar": (-38, 33),
        "apr": (-29, 36),
        "may": (-13, 40),
        "jun": (-8, 44),
        "jul": (-5, 46),
        "aug": (-6, 44),
        "sep": (-13, 43),
        "oct": (-18, 37),
        "nov": (-33, 33),
        "dec": (-39, 25),
    }

    def __init__(self, temp, units, record_date=None):
        """
        Initializes the Temperature object.

        Parameters:
        temp (float): Temperature value.
        units (str): The unit of measurement ('C', 'F', 'K').
        record_date (datetime, optional): The date of the record.
        """
        self.logger = ewx_utils_logger(log_path = ewx_log_file)
        self.logger.debug("Initializing Temperature object with temp: %s, units: %s, record_date: %s",
                          temp, units, record_date)
        self.record_date = record_date

        if temp is None:
            self.tempC = None
            self.tempF = None
            self.tempK = None
            self.logger.debug("Temperature is None, setting all temperature values to None")
            return

        unitsU = units.upper()
        nine_fifths = 9.0 / 5.0

        if unitsU == "C":
            self.tempC = float(temp)
            self.tempF = nine_fifths * self.tempC + 32
            self.tempK = self.tempC + 273.15
        elif unitsU == "F":
            self.tempF = float(temp)
            self.tempC = (self.tempF - 32) / nine_fifths
            self.tempK = self.tempC + 273.15
        elif unitsU == "K":
            self.tempK = float(temp)
            self.tempC = self.tempK - 273.15
            self.tempF = nine_fifths * self.tempC + 32
        else:
            self.logger.error("Invalid units provided: %s", units)
            raise ValueError("Units must be 'C', 'F', or 'K'")

        self.logger.debug("Temperature object initialized with tempC: %s, tempF: %s, tempK: %s",
                          self.tempC, self.tempF, self.tempK)
        
    def set_src(self, src):
        """
        Initializes the source of temperature data.

        Parameters:
        src(str): Source of the data
        """
        self.src = src
        self.logger.debug("Source set to: %s, src")

    def is_valid(self):
        """
        Checks if the temperature value is within the valid hourly default monthly range.

        Returns:
        bool: True if the temperature value is within the valid range, False otherwise.
        """
        self.logger.debug("Validating temperature value: %s, record_date: %s", self.tempC, self.record_date)

        if self.tempC is None:
            self.logger.debug("Temperature is None, returning False")
            return False

        if self.record_date is not None:
            month_abbrv = self.record_date.strftime("%b").lower()
            atmp_valid_range = self.valid_hourly_atmp.get(month_abbrv)
            if atmp_valid_range:
                is_valid = atmp_valid_range[0] <= self.tempC <= atmp_valid_range[1]
                self.logger.debug("Temperature value: %s is valid for month %s: %s",
                                  self.tempC, month_abbrv, is_valid)
                return is_valid

        is_valid = -40 <= self.tempC <= 46
        self.logger.debug("Temperature value: %s is within general valid range (-40 to 46): %s",
                          self.tempC, is_valid)
        return is_valid

