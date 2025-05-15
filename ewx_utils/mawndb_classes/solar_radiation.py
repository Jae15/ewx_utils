import os
import sys
from dotenv import load_dotenv
load_dotenv()
ewx_base_path = os.getenv("EWX_BASE_PATH")
sys.path.append(ewx_base_path)
from datetime import datetime
from ewx_utils.ewx_config import ewx_log_file
from ewx_utils.logs.ewx_utils_logs_config import ewx_unstructured_logger
from ewx_utils.logs.ewx_utils_logs_config import EWXStructuredLogger

class SolarRadiation:
    """
    The Solar Radiation class defines the valid ranges for solar radiation
    It is used to validate solar radiation data retrieved from mawndb.
    """

    # Valid solar radiation ranges for hourly and daily tables
    valid_srad_hourly_default = (0, 4500)
    valid_srad_daily_default = (0, 32000)

    def __init__(self, srad, table: str, record_date=None):
        """
        Initializes the Solar Radiation object.

        Parameters:
        srad(float): Solar radiation value.
        table(str): The type of table('HOURLY', or 'DAILY')
        record_date(datetime, optional): The date of the record.
        """
        self.logger = EWXStructuredLogger(log_path = ewx_log_file)
        self.logger.debug("Initializing Solar Radiation object with srad: %s, table: %s, record_date: %s", 
                          srad, table, record_date)
        self.record_date = record_date
        self.src = None
        self.tableU = table.upper()
        
        if srad is None:
            srad = -9999
        self.srad = float(srad)
        
        self.logger.debug("Solar Radiation object initialized with srad: %s", self.srad)
            
    def set_src(self, src):
        """
        Sets the source of the solar radiation data.
        
        Parameters:
        src(str): The source of the data
        """
        self.logger.debug("Setting source to: %s", src)
        self.src = src

    def is_valid(self):
        """
        Checks if the solar radiation value is within the valid range for the specified table type.

        Returns:
        bool: True if the solar radiation value is within the valid range, False otherwise.
        """
        self.logger.debug("Validating solar radiation value: %s for table: %s",
                          self.srad, self.tableU)
        
        # Check if srad is None or invalid
        if self.srad == -9999:
            self.logger.debug("Solar radiation value is invalid (-9999), returning False.")
            return False
        
        if self.tableU == "HOURLY":
            validation_range = self.valid_srad_hourly_default
        elif self.tableU == "DAILY":
            validation_range = self.valid_srad_daily_default
        else:
            self.logger.error("Invalid table type: %s", self.tableU)
            return False
            
        is_valid = validation_range[0] <= self.srad <= validation_range[1]
        self.logger.debug("Solar radiation value: %s is valid: %s", self.srad, is_valid)
        return is_valid