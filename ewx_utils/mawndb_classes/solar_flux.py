import os
import sys
from dotenv import load_dotenv
load_dotenv()
ewx_base_path = os.getenv("EWX_BASE_PATH")
sys.path.append(ewx_base_path)
from datetime import datetime
from ewx_utils.ewx_config import ewx_log_file
from ewx_utils.logs.ewx_utils_logs_config import ewx_utils_logger

class SolarFlux:
    """
    The SolarFlux class defines the valid ranges for max solar flux (sden_max) measurements.
    Max solar flux is primarily measured in W/m² (WPMS) and van be converted to kJ/m²/min or Langleys/min.
    """
    # Valid range for max solar flux (sden_max) in in W/m²
    valid_sden_max_default =(0, 105)

    # Conversion factors
    WPMS_TO_KJPMS = 60 / 1000  # 1 W/m² = 0.06 kJ/m²/min
    WPMS_TO_LY = 60 / 1000 * 0.239  # 1 W/m² = 0.01434 ly/min (Langleys per minute)

    def __init__(self, sden_max, units, record_date=None):
        """
        Initializes the SolarFlux Object.

        Parameters:
        sden_max (float): Max solar flux value.
        table (str): The type of table ('FIVEMIN', 'HOURLY', or 'DAILY')
        units (str): The unit of measurement('WPMS', 'KJPMS', or 'LY')
                    'WPMS' for watts per square meter
                    'KJPMS' for kilojoules per square meter per minute
                    'LY' for Langleys per minute.

        record_date (datetime, optional): The date of the record.
        """
        self.logger = ewx_utils_logger(log_path=ewx_log_file)
        self.logger.debug("Initializing SolarFlux with sden_max: %s, units: %s, record_date: %s",
                          sden_max, units, record_date)
        
        self.record_date = record_date
        self.src = None

        # Handle None values
        if sden_max is None:
            sden_max = -9999

        unitsU = units.upper()
        if unitsU == "WPMS":
            self.sdenWPMS = float(sden_max)
            self.sdenKJPMS = float(sden_max) * 60 / 1000
            self.sdenLY = float(sden_max) * 60 / 1000 * 0.239
        elif unitsU == "KJPMS":
            self.sdenKJPMS = float(sden_max)
            self.sdenWPMS = float(sden_max) * 1000 / 60
            self.sdenLY = float(sden_max) * 0.239
        elif unitsU == "LY":
            self.sdenLY = float(sden_max)
            self.sdenKJPMS = float(sden_max) / 0.239
            self.sdenWPMS = float(sden_max) / 0.239 * 1000 / 60
        else:
            self.logger.error("Invalid units provided: %s", units)
            raise ValueError("Units must be either 'WPMS', 'KJPMS' or 'LY'")
    
    def set_src(self, src):
        """
        Sets the source of the solar flux data.

        Parameters:
        src (str): Source of the data.
        """
        self.logger.debug("Setting source to: %s", src)
        self.src = src
    
    def is_valid(self):
        """
        Checks if the max solar flux is within the valid range.

        Returns:
        bool: True if the max solar flux value is within the valid range, False otherwise.
        """

        self.logger.debug("Validating max solar flux value: %s W/m²", self.sdenWPMS)

        # Use the default validation range
        validation_range = self.valid_sden_max_default

        if validation_range[0] <= self.sdenWPMS <= validation_range[1]:
            self.logger.debug("Max solar flux value: %s W/m² is within the valid range: %s",
                              self.sdenWPMS, validation_range)
            return True
        else:
            self.logger.debug("Max solar flux value: %s W/m² is outside the valid range: %s",
                              self.sdenWPMS, validation_range)
            return False




