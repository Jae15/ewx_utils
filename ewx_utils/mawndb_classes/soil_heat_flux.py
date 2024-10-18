import sys
sys.path.append("c:/Users/mwangija/data_file/ewx_utils/ewx_utils")
from mawndb_classes.mawndb_classes_logs_config import mawndb_classes_logger

class SoilHeatFlux:
    """
    The SoilHeatFlux class defines the valid hourly default range for soil heat flux.
    It is used to validate soil heat flux data retrieved from mawndb.
    """
    valid_sflux_hourly_default = (0, 7000)

    def __init__(self, sflux, record_date=None):
        """
        Initializes the SoilHeatFlux.

        Parameters:
        sflux(float): Soil heat flux value
        record_date(datetime, optional): The date of the record.
        """
        self.logger = mawndb_classes_logger()
        self.logger.debug("Initializing SoilHeatFlux object with sflux: %s, record_date: %s", sflux, record_date)
        
        self.record_date = record_date
        self.sflux = sflux

    def is_valid(self):
        """
        Checks if the soil heat flux value is within the valid hourly default range.

        Returns:
        bool: True if the soil heat flux value is within the valid range, False otherwise.
        """
        self.logger.debug("Validating soil heat flux value: %s", self.sflux)

        # Check if mstr is None
        if self.sflux is None:
            self.logger.debug("Soil heat flux value is None, returning False.")
            return False

        is_valid = self.valid_sflux_hourly_default[0] <= self.sflux <= self.valid_sflux_hourly_default[1]
        self.logger.debug("Soil moisture value: %s is valid: %s", self.sflux, is_valid)

        return is_valid
    
