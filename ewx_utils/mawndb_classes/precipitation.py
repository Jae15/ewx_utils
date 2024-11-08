import os
import sys
import dotenv
dotenv.load_dotenv()
from dotenv import load_dotenv
load_dotenv()
ewx_base_path = os.getenv("EWX_BASE_PATH")
sys.path.append(ewx_base_path)
from datetime import datetime
from ewx_utils.ewx_config import (
    ewx_base_path, ewx_log_file)
from ewx_utils.logs.ewx_utils_logs_config import ewx_utils_logger

class Precipitation:
    """
The precipitation class below defines the valid hourly default monthly ranges.
It also specifies the units of measurement and their respective conversions and tables as stored in mawndb.
    """
    valid_pcpn_fivemin_default = (0, 14)
    valid_pcpn_hourly_default = (0, 77)
    valid_pcpn_daily_default = (0, 254)

    def __init__(self, pcpn, table: str, units, record_date=None):
        """
        Initializes the Precipitation object.

        Parameters:
        pcpn(float): Precipitation value.
        table(str): THe type of table('FIVEMIN','HOURLY','DAILY').
        units(str): The unit of measurement('MM' or 'IN')

        """
        self.logger = ewx_utils_logger(log_path = ewx_log_file)
        self.logger.debug("Initializing Precipitation object with pcpn: %s, table: %s, units: %s, record_date: %s",
                          pcpn, table, units, record_date)
        self.record_date = record_date
        self.src = None
        self.tableU = table.upper()
        if pcpn is None:
            pcpn = -9999
        unitsU = units.upper()
        if unitsU == "MM":
            self.pcpnMM = float(pcpn)
            self.pcpnIN = float(pcpn) / 25.4
        elif unitsU == "IN":
            self.pcpnMM = float(pcpn) * 25.4
            self.pcpnIN = float(pcpn)
        else:
            self.logger.debug("Precipitation object initialized with pcpnMM: %s and pcpnIN: %s",
                              self.pcpnMM, self.pcpnIN)


    def set_src(self, src):
        """
        Sets the source of the precipitation data.

        Parameters:
        src(str): Source of the data.
        """
        self.src = src
        self.logger.debug("Source set to: %s, src")

    def is_valid(self):
        """
         Checks if the precipitation value is within the valid range for the specified table type.

         Returns:
         bool: True if the precipitation value is within the valid range, False otherwise

        """
        self.logger.debug("Validating precipitation value: %s for table type: %s",
                          self.pcpnMM, self.tableU)
       
        if self.tableU == "FIVEMIN":
            validation_range = self.valid_pcpn_fivemin_default
        elif self.tableU == "HOURLY":
            validation_range = self.valid_pcpn_hourly_default
        elif self.tableU == "DAILY":
            validation_range = self.valid_pcpn_daily_default
        else:
            self.logger.error("Invalid table type provided: %s", self.tableU)
            raise ValueError("Table must be either 'FIVEMIN', 'HOURLY', or 'DAILY'")
        is_valid = validation_range[0] <= self.pcpnMM <= validation_range[1]
        self.logger.debug("Precipitation value: %s is valid: %s",
                          self.pcpnMM, is_valid)
        
        return is_valid 
