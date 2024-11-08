from mawndb_classes.evapotranspiration import Evapotranspiration
from mawndb_classes.precipitation import Precipitation
from mawndb_classes.winddirection import WindDirection
from mawndb_classes.temperature import Temperature
from mawndb_classes.leafwetness import LeafWetness
from mawndb_classes.humidity import Humidity
from mawndb_classes.windspeed import WindSpeed
from variables_list import relh_vars
from variables_list import temp_vars
from variables_list import pcpn_vars
from variables_list import rpet_vars
from variables_list import wspd_vars
from variables_list import wdir_vars
from variables_list import leafwt_vars
from variables_list import mstr_vars
from variables_list import nrad_vars
from variables_list import srad_vars
from datetime import datetime
import datetime as dt
from datetime import datetime
import ewx_utils.ewx_config as ewx_config
from logs.ewx_utils_logs_config import ewx_utils_logger
from validation_logsconfig import validations_logger

validation_logger = ewx_utils_logger(path = ewx_config.ewx_log_file)
validation_logger.error("Remember to log errors using my_logger")
# logger = logging.getLogger(__name__)

"""Defining the check value function below that takes in the variable key as a string, the value as a float and the date as datetime"""


def check_value(k: str, v: float, d: dt.datetime):
    # checking the relh_vars
    # If the key is in relh_vars
    if k in relh_vars:
        rh = Humidity(
            v, "PCT", d
        )  # Obtain the value, the unit and the date as created in  the class humidity
        if rh.IsValid() and rh.IsInRange():
            return True
        else:
            return False

    # checking the pcpn values
    # If the key is in pcpn_vars
    if k in pcpn_vars:
        pn = Precipitation(
            v, "hourly", "MM", d
        )  # Obtain the value, the table, the unit and the date
        if pn.IsValid():
            return True
        else:
            return False

    # checking the evapotranspiration values
    # If the key is found in the rpet_vars list
    if k in rpet_vars:
        rp = Evapotranspiration(
            v, "hourly", "MM", d
        )  # Obtain the value, the table, the unit and the date
        if rp.IsValid():
            return True
        else:
            return False

    # checking the temperature values
    # If the key is found in the temp_vars list
    if k in temp_vars:
        tp = Temperature(
            v, "c", d
        )  # Obtain the value, the unit and the date the value was retrieved
        if tp.IsValid():
            return True
        else:
            return False

    # checking the windspeed values
    # If the key is found in the wspd_vars list
    if k in wspd_vars:
        ws = WindSpeed(
            v, "MPS", d
        )  # Obtain the value, the unit and the date the value was retrieved
        if ws.IsValid():
            return True
        else:
            return False

    # checking the winddirection values
    # If the key found in the wdir_vars list
    if k in wdir_vars:
        wd = WindDirection(
            v, "DEGREES", d
        )  # Obtain the value, the unit and the date the value was retrieved
        if wd.IsValid():
            return True
        else:
            return False

    # checking the leafwetness values
    # If the key found in the leafwt_vars list
    if k in leafwt_vars:
        lw = LeafWetness(
            v, "hourly", k, d
        )  # Obtain the value, the table, the specific sensor ie lws01 or lws01 and the date
        if lw.IsValid() and lw.IsWet():
            return True
        else:
            return False

    return True
