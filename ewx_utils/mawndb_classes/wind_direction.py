import os
import sys
from dotenv import load_dotenv
load_dotenv()
ewx_base_path = os.getenv("EWX_BASE_PATH")
sys.path.append(ewx_base_path)
from datetime import datetime
from ewx_utils.ewx_config import ewx_log_file
from ewx_utils.logs.ewx_utils_logs_config import ewx_utils_logger

class WindDirection:
    """
    The WindDirection class defines the valid hourly default range for wind direction.
    It also specifies the units of measurement (degrees) and their respective compass directions.
    """
    valid_wdir_hourly_default = (0, 360)

    def __init__(self, wdir, units, record_date=None):
        """
        Initializes the WindDirection object.
        
        Parameters:
        wdir (float): Wind direction value.
        units (str): The unit of measurement ('DEGREES').
        record_date (datetime, optional): The date of the record.
        """
        self.logger = ewx_utils_logger(log_path = ewx_log_file)
        self.logger.debug("Initializing WindDirection object with wdir: %s, units: %s, record_date: %s",
                          wdir, units, record_date)

        self.record_date = record_date
        if wdir is None:
            self.wdirDEGREES = None
            self.wdirCOMPASS = None
            self.logger.debug("Wind direction is None, setting all direction values to None")
            return

        unitsU = units.upper()
        if unitsU == 'DEGREES':
            self.wdirDEGREES = float(wdir)
            self.wdirCOMPASS = self._degrees_to_compass(self.wdirDEGREES)
        else:
            self.logger.error("Invalid units provided: %s", units)
            raise ValueError("Units must be 'DEGREES'")

        self.logger.debug("WindDirection object initialized with wdirDEGREES: %s and wdirCOMPASS: %s",
                          self.wdirDEGREES, self.wdirCOMPASS)
    
    def set_src(self, src):
        """
        Initializes the source of wind direction data.

        Parameters:
        src(str): Source of the data
        """
        self.src = src
        self.logger.debug("Source set to: %s, src")

    def _degrees_to_compass(self, degrees):
        """
        Converts wind direction in degrees to compass direction.
        
        Parameters:
        degrees (float): Wind direction in degrees.
        
        Returns:
        str: Compass direction corresponding to the degrees.
        """
        if degrees < 0 or degrees > 360:
            return 'NA'
        compass = {
            (0, 11.25): 'N', (11.25, 33.75): 'NNE', (33.75, 56.25): 'NE', (56.25, 78.75): 'ENE',
            (78.75, 101.25): 'E', (101.25, 123.75): 'ESE', (123.75, 146.25): 'SE', (146.25, 168.75): 'SSE',
            (168.75, 191.25): 'S', (191.25, 213.75): 'SSW', (213.75, 236.25): 'SW', (236.25, 258.75): 'WSW',
            (258.75, 281.25): 'W', (281.25, 303.75): 'WNW', (303.75, 326.25): 'NW', (326.25, 348.75): 'NNW',
            (348.75, 360): 'N'
        }
        for (low, high), direction in compass.items():
            if low <= degrees < high:
                return direction
        return 'NA'

    def is_valid(self):
        """
        Checks if the wind direction value is within the valid hourly default range.
        
        Returns:
        bool: True if the wind direction value is within the valid range, False otherwise.
        """
        self.logger.debug("Validating wind direction value: %s", self.wdirDEGREES)
        if self.wdirDEGREES is None:
            self.logger.debug("Wind direction is None, returning False")
            return False

        is_valid = self.valid_wdir_hourly_default[0] <= self.wdirDEGREES <= self.valid_wdir_hourly_default[1]
        self.logger.debug("Wind direction value: %s is valid: %s", self.wdirDEGREES, is_valid)
        return is_valid

