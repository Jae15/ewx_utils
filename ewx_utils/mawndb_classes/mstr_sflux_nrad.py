import sys
import logging
sys.path.append("c:/Users/mwangija/data_file/ewx_utils/ewx_utils")
from mawndb_classes_logs_config import mawndb_classes_logger

class SoilMoisture:
    """
    The SoilMoisture class defines the valid hourly default ranges for soil moisture
    """
    valid_mstr_hourly_default = (0, 1)

    def __init__(self, mstr, record_date=None):
        """
        Initializes the SoilMoisture object.

        Parameters:
        mstr(float): Soil moisture value.
        record_date(datetime, optional): The date of the record.
        
        """
        self.logger = mawndb_classes_logger()
        self.logger.debug("Initializing SoilMoisture object with mstr: %s, record_date: %s", mstr, record_date)

        self.record_date = record_date
        self.mstr = mstr

    def is_valid(self):
        """
        Checks if the soil moisture value is within  the valid hourly default range.

        Returns:
        bool: True if the soil moisture value is within the valid range, False otherwise.
        """
        self.logger.debug("Validating soil moisture value: %s", self.mstr)
        is_valid = self.valid_mstr_hourly_default[0] < self.mstr < self.valid_mstr_hourly_default[1]
        self.logger.debug("Soil moisture value: %s is valid: %s", self.mstr, is_valid)
        return is_valid


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
        is_valid = self.valid_sflux_hourly_default[0] < self.sflux < self.valid_sflux_hourly_default[1]
        self.logger.debug("Soil heat flux value: %s is valid: %s", self.sflux, is_valid)
        return is_valid
    
class NetRadiation:
    """
    The NetRadiation class defines the valid hourly default range for net radiation
    It is used to validate net radiation data retrieved from mawndb.

    """

    valid_nrad_hourly_default = (-1250, 1250)

    def __init__(self, nrad, record_date=None):
        """
        Initializes the NetRadiation object.

        Parameters:
        nrad(float): Net radiation value.
        record_date(datetime, optional): The date of the record.
        
        """
        self.logger = mawndb_classes_logger()
        self.logger.debug("Initializing NetRadiation object with nrad: %s, record_date: %s", nrad, record_date)
        self.record_date = record_date
        self.nrad = nrad

    def is_valid(self):
        """
        Checks if the net radiation value is within the valid hourly default range

        Returns:
        bool: True if the net radiation value is within the valid range, False otherwise

        """
        self.logger.debug("Validating net radiation value: %s", self.nrad)
        is_valid = self.valid_nrad_hourly_default[0] < self.nrad < self.valid_nrad_hourly_default[1]
        self.logger.debug("Net radiation value: %s is valid: %s", self.nrad, is_valid)
        return is_valid
