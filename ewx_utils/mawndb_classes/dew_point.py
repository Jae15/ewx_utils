import os
import sys
import dotenv
import math
dotenv.load_dotenv()
ewx_base_path = os.getenv("EWX_BASE_PATH")
sys.path.append(ewx_base_path)
from ewx_utils.ewx_config import ewx_base_path, ewx_database_configfile, ewx_log_file
from datetime import datetime
import ewx_utils.ewx_config as ewx_config
from ewx_utils.logs.ewx_utils_logs_config import ewx_utils_logger
from ewx_utils.mawndb_classes.humidity import Humidity
from ewx_utils.mawndb_classes.temperature import Temperature

class DewPoint:
    """
    The DewPoint class calculates the dew point temperature from humidity and temperature measurements.
    It relies on the validity of the Humidity and Temperature classes.
    """

    def __init__(self, temp, relh, record_date = None) -> None:
        """
        Initializes the DewPoint object.

        Parameters:
        temp(Temperature): A Temperature object.
        relh(Humidity): A Humidity object.
        """
        self.logger = ewx_utils_logger(log_path = ewx_log_file)
        self.logger.debug("Initializing DewPoint object with temp: %s and relh: %s", temp, relh)

        if not isinstance(temp, Temperature) or not isinstance(relh, Humidity):
            self.logger.error("Invalid temperature or humidity object passed.")
            raise TypeError("Temperature and Humidity must be instances of their respective classes.")
        self.temp = temp
        self.relh = relh

        if not self.relh.is_valid():
            self.logger.error("Invalid humidity value: %s", self.relh.relhPCT)
            raise ValueError("Invalid humidity value. ")
        if not self.temp.is_valid():
            self.logger.error("Invalid temperature value: %s", self.temp.tempC)
            raise ValueError("Invalid temperature value.")
        
    def set_src(self, src):
        """
        Sets the source of dew point data.

        Parameters:
        str(str): Source of the data. 
        """
        self.src = src
        self.logger.debug("Source set to: %s src")

    def calculate_dew_point(self):
        """
        Calculates dew point temperature 
        Returns:
        float: 
        The calculated dew point in temperature
        """
        self.logger.debug("Calculating dew point.")
        
        tempC = self.temp.tempC
        relhPCT = self.relh.relhPCT

        # Calculating the saturated vapor pressure
        saturated_vapor = round(0.61078 *math.exp((17.269 * tempC) / (tempC + 237.3)), 6)
        # Calculating actual vapor pressure
        dew_point_vapor = round(relhPCT / 100 * saturated_vapor, 6)
        # Calculating dew point temperature
        dwptC = round((116.9 + 237.3 * math.log(dew_point_vapor)) / (16.78 - math.log(dew_point_vapor)), 3)
        return dwptC
