import sys
import logging
from datetime import datetime
sys.path.append("c:/Users/mwangija/data_file/ewx_utils/ewx_utils")

from mawndb_classes.mawndb_classes_logs_config import mawndb_classes_logger

class VaporPressure:
    """
    The class vapor pressure below defines the valid hourly default monthly ranges
    """
    # Valid range for vapor pressure measurements
    valid_vapr_hourly_default = (0, 4)

    def __init__(self, vapr, units, record_date = None):
        """
        Initializes the vapor pressure object

        Parameters:
        vapr(float): Vapor Pressure Value
        units(str): The unit of measurement ('KPA')
        record_date(datetime, optional): The date of the record
        """
        self.logger = mawndb_classes_logger()
        self.logger.debug("Initializing Vapor Pressure object with vapr: %s, units: %s, record_date: %s", 
                          vapr, record_date)
        
        self.record_date = record_date
        self.vaprKPA = vapr
        if vapr is None:
            vapr = -9999
            unitsU = units.upper()
            if unitsU == "KPA":
                self.vapr = float(vapr)
    
    def set_src(self, src):
        """
        Initializes the source of vapor pressure data.

        Parameters:
        src(str): Source of the data
        """

        self.src = src
        self.logger.debug("Source set to: %s, src")
    
    def is_valid(self):
        if (
            self.vaprKPA >= self.valid_vapr_hourly_default[0]
            and self.vaprKPA <= self.valid_vapr_hourly_default[1]
        ):
            return True
        else:
            return False
