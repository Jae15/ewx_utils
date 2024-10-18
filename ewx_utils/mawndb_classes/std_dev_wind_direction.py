import sys
sys.path.append("c:/Users/mwangija/data_file/ewx_utils/ewx_utils")
from mawndb_classes.mawndb_classes_logs_config import mawndb_classes_logger
class StdDevWindDirection:
    """
    The StdDevWindDirection class defines the valid hourly default range for standard deviation of wind direction.
    It is used to validate standard deviation of wind direction data retrieved from mawndb.
    """
    valid_wstdv_hourly_default = (0, 99)

    def __init__(self, wstdv, units, record_date=None):
        """
        Initializes the StdWindDirection object.

        Parameters:
        wstdv(float): Standard deviation of wind direction value.
        units(str): The unit of measurement ('DEGREES').
        record_date(datetime, optional): The date of the record.

        """
        self.logger = mawndb_classes_logger()
        self.logger.debug("Initializing StdDevWindDirection object with wstdv: %s, units: %s, record_date: %s",
                          wstdv, units, record_date)
        
        self.record_date = record_date
        self.wstdvM = wstdv 
        if wstdv is None:
            wstdv = -9999
            unitsU = units.upper()
            if unitsU == "DEGREES":
                self.wstdvM = float(wstdv)

    def set_src(self, src):
        """
        Initializes the source of std wind direction data.

        Parameters:
        src(str): Source of the data
        """
        self.src = src
        self.logger.debug("Source set to: %s, src")
        
    def is_valid(self):
        """
        Checks if the standard wind direction value is within the valid hourly default range.
        
        Returns:
        bool: True if the wind direction value is within the valid range, False otherwise.
        """
        self.logger.debug("Validating standard deviation of wind direction value: %s", self.wstdvM)
        if self.wstdvM is None:
            self.logger.debug("Standard Deviation of Wind direction is None, returning False")
            return False

        is_valid = self.valid_wstdv_hourly_default[0] <= self.wstdvM <= self.valid_wstdv_hourly_default[1]
        self.logger.debug("Standard Deviation of Wind direction value: %s is valid: %s", self.wstdvM, is_valid)
        return is_valid

