import sys
import logging
sys.path.append("c:/Users/mwangija/data_file/ewx_utils/ewx_utils")
from mawndb_classes_logs_config import mawndb_classes_logger
class StdWindDirection:
    """
    The StdWindDirection class defines the valid hourly default range for stndard wind direction.
    It is used to validate wind direction data retrieved from mawndb.
    """
    valid_wstdv_hourly_default = (0, 99)

    def __init__(self, wstdv, units, record_date=None):
        """
        Initializes the StdWindDirection object.

        Parameters:
        wstdv(float): Standard wind direction value.
        units(str): The unit of measurement ('DEGREES').
        record_date(datetime, optional): The date of the record.

        """
        self.logger = mawndb_classes_logger()
        self.logger.debug("Initializing StdWindDirection object with wstdv: %s, units: %s, record_date: %s",
                          wstdv, units, record_date)
        
        self.record_date = record_date
        self.wstdvM = wstdv 
        if wstdv is None:
            wstdv = -9999
            unitsU = units.upper()
            if unitsU == "DEGREES":
                self.wstdvM = float(wstdv)

    def is_valid(self):
        if (
            self.wstdvM < self.valid_wstdv_hourly_default[0]
            and self.wstdvM > self.valid_wstdv_hourly_default[1]
        ):
            return True
        else:
            return False


