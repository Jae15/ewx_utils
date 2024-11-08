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

class LeafWetness:
    """
Below is the precipitation class validation code which defines the valid fivemin, hourly and daily default monthly ranges.
The units of measurements as stored in mawndb and their respective conversions, sensors and tables are also specified.
    """

    def __init__(self, lw, table, sensor, record_date=None):
        """
        Initializes the Leafwetness object.

        Parameters:
        lw(float): Leaf wetness value.
        table(str): The type of table ('FIVEMIN', or 'HOURLY').
        sensor(str): The type of sensor('LEAF0','LEAF1', 'LWS0', 'LWS1').
        record_date(datetime, optiona): The date of the record.
        """
        self.logger = ewx_utils_logger(log_path = ewx_log_file)
        self.logger.debug("Initializing Leafwetness object with lw: %s, table: %s, sensor: %s, record_date: %s",
                          lw, table, sensor, record_date)
        self.record_date = record_date
        if lw is None:
            lw = -9999  # what would it take to keep this as None?
        else:
            self.lw = lw
        self.tableU = table.upper() # The type of table is capitalized
        self.sensorU = sensor.upper() # The sensor type is capitalized
        if isinstance(self.lw, (int, float)):
            self.percent = str(int(self.lw * 100 + 0.5))  # only makes sense for hourly.
        else:
            self.percent = -9999
        self.logger.debug("LeafWetness object initialized with lw: %s, percent: %s, tableU: %s",
                          self.lw, self.tableU, self.sensorU)
        # if tableU = 'FIVEMIN':
        # (valid if > 0), wet if < 5000
        # NEED TO DEAL WITH HOURLY ALSO.
    def set_src(self, src):
        """
        Sets the source of Leaf Wetness data.

        Parameters:
        src(str): Source of the data. 
        """
        self.src = src
        self.logger.debug("Source set to: %s src")
    

    def is_valid(self):
        """
        Checks if the leaf wetness value is within the valid range for the specified table type and sensor

        Returns:
        bool: True if the leaf wetness value is within the valid range, False otherwise

        """
        
        self.logger.debug("Validating leaf wetness value: %s for the table type: %s and sensor: %s",
                          self.lw, self.tableU, self.sensorU)
        if self.tableU == "FIVEMIN":
            if self.sensorU in ['LEAF0', 'LEAF1']:
                is_valid = -100 <= self.lw < 9999
            else: # lws0 or lws1
                is_valid = True # All valid valid/no valid range for these sensors
        elif self.tableU == "HOURLY":
                is_valid = 0 <= self.lw <= 1
        else:
            self.logger.error("Invalid table type provided: %s", self.tableU)
            raise ValueError("Table must be either 'FIVEMIN' or 'HOURLY'")
        self.logger.debug("Leaf wetness value: %s is valid: %s", self.lw, is_valid)
        return is_valid

    def is_wet(self):
        if self.tableU == "FIVEMIN":
            if self.sensorU in ['LEAF0', 'LEAF1']:
                is_wet = self.is_valid() and -100 <= self.lw < 9999
            else: # lws0 or lws1
                is_wet = self.is_valid() and self.lw >= 280
        elif self.tableU == "HOURLY":
            is_wet = self.is_valid() and self.lw >= 0.25
        else:
            is_wet = False # In case table type is invalid
        
        self.logger.debug("Leaf wetness determination for lw: %s is wet: %s", self.lw, is_wet)
        return is_wet