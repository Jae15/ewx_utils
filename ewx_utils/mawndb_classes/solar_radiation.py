import sys
sys.path.append("c:/Users/mwangija/data_file/ewx_utils/ewx_utils")
from mawndb_classes.mawndb_classes_logs_config import mawndb_classes_logger

class SolarRadiation:
    """
    The Solar Radiation class defines the valid hourly default range for solar radiation
    It is used to validate solar radiation data retrieved from mawndb.
    """

    valid_srad_hourly_default = (0, 4500)

    def __init__(self, srad, record_date=None):
        """
        Initializes the Solar Radiation object.

        Parameters:
        nrad(float): Net radiation value.
        record_date(datetime, optional): The date of the record.

        """
        self.logger = mawndb_classes_logger()
        self.logger.debug("Initializing Solar Radiation object with srad: %s, record_date: %s", srad, record_date)
        self.record_date = record_date
        self.srad = srad

        """
        if self.srad is not None:
            self.srad = round(srad, 5)
        else:
            self.srad = None
        """

    def is_valid(self):
        """
        Checks if the net radiation value is within the valid hourly default range.

        Returns:
        bool: True if the net radiation value is within the valid range, False otherwise.
        """
        self.logger.debug("Validating net radiation value: %s", self.srad)

        # Check if nrad is None
        if self.srad is None:
            self.logger.debug("Net radiation value is None, returning False.")
            return False

        is_valid = self.valid_srad_hourly_default[0] <= self.srad <= self.valid_srad_hourly_default[1]
        self.logger.debug("Net radiation value: %s is valid: %s", self.srad, is_valid)
        return is_valid



""" Solar radiation variables list - if the k - key - is in this list, treat the variable as 'srad_vars' """

# srad_vars = ["srad"]