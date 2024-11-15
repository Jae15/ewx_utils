import os
import sys
from dotenv import load_dotenv
load_dotenv()
ewx_base_path = os.getenv("EWX_BASE_PATH")
sys.path.append(ewx_base_path)
from ewx_utils.ewx_config import ewx_log_file
from ewx_utils.logs.ewx_utils_logs_config import ewx_utils_logger

class Evapotranspiration:
    """ 
The evapotranspiration class below defines the valid hourly default monthly ranges.
It also specifies the units of measurement and their respective conversions as stored in mawndb.
    """

    # Valid rpet ranges for hourly and daily tables
    valid_rpet_hourly_default = (0, 10)
    valid_rpet_daily_default = (0, 10)

    def __init__(self, rpet, table: str, units, record_date=None):
        """
        Initializes the Evapotranspiration object.

        Parameters:
        rpet(float): Reference potential evapotranspiration value
        table(str): The type of table('HOURLY', or 'DAILY')
        units(str): The unit of measurement ('MM' or 'IN')
        record_date(datetime, optional): The date of the record.
        """
        self.logger = ewx_utils_logger(log_path = ewx_log_file)
        self.logger.debug("Initializing Evapotranspiration object with rpet: %s, table: %s, units: %s, record_date: %s",
                          rpet, table, units, record_date)
        self.record_date = record_date
        self.src = None
        self.tableU = table.upper()
        if rpet is None:
            rpet = -9999
        unitsU = units.upper()
        if unitsU == "MM":
            self.rpetMM = float(rpet)
            self.rpetIN = float(rpet) / 25.4
        elif unitsU == "IN":
            self.rpetMM = float(rpet) * 25.4
            self.rpetIN = float(rpet)
        else:
            self.logger.error("Invalid units provided: %s, units")
            raise ValueError("Units must be either 'MM' or 'IN'")
        self.logger.debug("Evapotranspiration object initialized with rpet in MM: %s and IN: %s",
                              self.rpetMM, self.rpetIN)
            
    def set_src(self, src):
        """
        Sets the source of the evapotranspiration data.
        
        Parameters:
        src(str): The source of the data

        """
        self.logger.debug("Setting source to: %s, src")
        self.src = src

    def is_valid(self):
        """
        Checks if the rpet value is within the valid range of for the specified table type

        Returns:
        bool: True if the rpet value is within the valid range, False otherwise

        """

        self.logger.debug("Validating rpet value: %s for table: %s",
                          self.rpetMM, self.tableU)
        
        if self.tableU == "HOURLY":
            validation_range = self.valid_rpet_hourly_default
        elif self.tableU == "DAILY":
            validation_range = self.valid_rpet_daily_default
        else:
            self.logger.error("Invalid table type provided: %s", self.tableU)
            raise ValueError("Table must be either 'HOURLY or 'DAILY'")
        
        if validation_range[0] <= self.rpetMM <= validation_range[1]:
            self.logger.debug("rpet value: %s is within the valid range: %s", 
                              self.rpetMM, validation_range)
            return True
        else:
            self.logger.debug("rpet value: %s is outside the valid range: %s",
                              self.rpetMM, validation_range)
            return False
