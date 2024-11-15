import os
import sys
from dotenv import load_dotenv
load_dotenv()
ewx_base_path = os.getenv("EWX_BASE_PATH")
sys.path.append(ewx_base_path)
from datetime import datetime
from ewx_utils.ewx_config import ewx_log_file
from ewx_utils.logs.ewx_utils_logs_config import ewx_utils_logger

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
        self.logger = ewx_utils_logger(log_path = ewx_log_file)
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
    
