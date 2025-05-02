import os
import sys
import traceback
from datetime import datetime, date, timedelta
from psycopg2.extras import RealDictRow
import datetime
from dateutil import tz
from dotenv import load_dotenv
load_dotenv()
ewx_base_path = os.getenv("EWX_BASE_PATH")
sys.path.append(ewx_base_path)
from ewx_utils.ewx_config import ewx_log_file
from ewx_utils.logs.ewx_utils_logs_config import ewx_utils_logger
from typing import Dict, Optional, Tuple, Any, List
from .daily_time_utils import generate_list_of_dates
from .daily_variables_list import (
    relh_vars,
    pcpn_vars,
    rpet_vars,
    temp_vars,
    wspd_vars,
    wdir_vars,
    leafwt_vars,
    dwpt_vars,
    vapr_vars,
    mstr_vars,
    srad_vars,
    nrad_vars,
    sflux_vars,
    wstdv_vars,
    volt_vars,
    sden_vars
)
from ewx_utils.mawndb_classes.voltage import Voltage
from ewx_utils.mawndb_classes.humidity import Humidity
from ewx_utils.mawndb_classes.dew_point import DewPoint
from ewx_utils.mawndb_classes.wind_speed import WindSpeed
from ewx_utils.mawndb_classes.leaf_wetness import LeafWetness
from ewx_utils.mawndb_classes.temperature import Temperature
from ewx_utils.mawndb_classes.soil_moisture import SoilMoisture
from ewx_utils.mawndb_classes.net_radiation import NetRadiation
from ewx_utils.mawndb_classes.soil_heat_flux import SoilHeatFlux
from ewx_utils.mawndb_classes.precipitation import Precipitation
from ewx_utils.mawndb_classes.wind_direction import WindDirection
from ewx_utils.mawndb_classes.vapor_pressure import VaporPressure
from ewx_utils.mawndb_classes.solar_radiation import SolarRadiation
from ewx_utils.mawndb_classes.evapotranspiration import Evapotranspiration
from ewx_utils.mawndb_classes.std_dev_wind_direction import StdDevWindDirection
from ewx_utils.mawndb_classes.solar_flux import SolarFlux
from typing import List, Dict, Any, Optional, Tuple
from ewx_utils.logs.ewx_utils_logs_config import ewx_utils_logger

# Initialize logger
my_validation_logger = ewx_utils_logger(log_path=ewx_log_file)

def check_value(k: str, v: float, d: date) -> bool:
    """
    Checks if a given value is valid based on its variable key and date.
    Parameters:
        k (str): The variable key.
        v (float): The value to check.
        d (datetime.date): The date of the value.
    Returns:
        bool: True if the value is valid, False otherwise.
    """
    if k in relh_vars:
        rh = Humidity(v, "PCT", d)
        #print(k, v, d, rh.is_valid())
        return rh.is_in_range() and rh.is_valid()
    if k in pcpn_vars:
        pn = Precipitation(v, "daily", "MM", d)
        #print(k, v, d, pn.is_valid())
        return pn.is_valid()
    if k in rpet_vars:
        rp = Evapotranspiration(v, "daily", "MM", d)
        #print(k, v, d, rp.is_valid())
        return rp.is_valid()
    if k in temp_vars:
        tp = Temperature(v, "C", d)
        #print(k, v, d, tp.is_valid())
        return tp.is_valid()
    if k in wspd_vars:
        ws = WindSpeed(v, "MPS", d)
        #print(k, v, d, ws.is_valid())
        return ws.is_valid()
    if k in wdir_vars:
        wd = WindDirection(v, "DEGREES", d)
        #print(k, v, d, wd.is_valid())
        return wd.is_valid()
    if k in leafwt_vars:
        lw = LeafWetness(v, "daily", k, d)
        #print(k, v, d, lw.is_valid())
        return lw.is_valid()
    if k in dwpt_vars:
        dp = Temperature(v, "C", d)
        #print(k, v, d, dp.is_valid())
        return dp.is_valid()
    if k in vapr_vars:
        vp = VaporPressure(v, "daily", d)
        #print(k, v, d, vp.is_valid())
        return vp.is_valid()
    if k in mstr_vars:
        ms = SoilMoisture(v, d)
        #print(k, v, d, ms.is_valid())
        return ms.is_valid()
    if k in nrad_vars:
        nr = NetRadiation(v, d)
        #print(k, v, d, nr.is_valid())
        return nr.is_valid()
    if k in srad_vars:
        sr = SolarRadiation(v, d)
        #print(k, v, d, sr.is_valid())
        return sr.is_valid()
    if k in sflux_vars:
        sf = SoilHeatFlux(v, d)
        #print(k, v, d, sf.is_valid())
        return sf.is_valid()
    if k in wstdv_vars:
        wv = StdDevWindDirection(v, d)
        #print(k, v, d, wv.is_valid())
        return wv.is_valid()
    if k in volt_vars:
        vt = Voltage(v, d)
        #print(k, v, d, vt.is_valid())
        return vt.is_valid()
    if k in sden_vars:
        sd = SolarFlux(v, "WPMS", d)
        return sd.is_valid
    return False

# Initialize lists for records and columns
record_keys = []
record_vals = []
db_columns = []
qc_values = []
clean_records = []

def extract_date_safe(record: Dict[str, Any]) -> Optional[date]:
    try:
        date_val = record.get("date")
        return date_val if isinstance(date_val, date) else None
    except Exception:
        return None

def extract_year_day_date(record: Dict[str, Any]) -> Tuple[Optional[int], Optional[int], Optional[date]]:
    try:
        return (
            int(record.get("year")) if "year" in record else None,
            int(record.get("day")) if "day" in record else None,
            record.get("date") if isinstance(record.get("date"), date) else None
        )
    except Exception:
        return (None, None, None)

def extract_record_components(record: Dict[str, Any]) -> Tuple[Optional[int], Optional[int], Optional[date]]:
    """
    Extract year, day, and date from a record safely.

    Returns:
        Tuple of (year, day, date) or (None, None, None) if invalid
    """
    try:
        year = int(record["year"]) if "year" in record else None
        day = int(record["day"]) if "day" in record else None
        dt = record.get("date") if isinstance(record.get("date"), date) else None
        return year, day, dt
    except Exception as e:
        my_validation_logger.error(f"Failed to extract record components: {e}")
        return None, None, None
    
def get_day_of_year(record: Dict[str, Any]) -> Optional[int]:
    try:
        dt = record.get("date")
        if isinstance(dt, date):
            return dt.timetuple().tm_yday
        return int(record.get("day")) if "day" in record else None
    except Exception as e:
        my_validation_logger.error(f"Error extracting day of year: {e}")
        return None

def get_year(record: Dict[str, Any]) -> Optional[int]:
    try:
        return int(record.get("year")) if "year" in record else None
    except Exception as e:
        my_validation_logger.error(f"Error extracting year: {e}")
        return None


def creating_mawnsrc_record(
        record: Dict[str, Any],
        id_col_list: List[str],
        date_of_record: Optional[date],
        default_source: str,
) -> Dict[str, Any]:
    """
    Create a MAWN source record with appropriate source indicators.
    """
    if not record or not id_col_list:
        my_validation_logger.error("Missing required record or id_col_list")
        raise ValueError("Record and id_col_list are required")

    if not date_of_record:
        _, _, date_of_record = extract_record_components(record)

    if not date_of_record:
        my_validation_logger.warning("Date is missing or invalid in creating_mawnsrc_record")
        raise ValueError("Invalid date_of_record")

    mawnsrc_record = record.copy()
    my_validation_logger.debug("Created copy of original record")

    for key in record.keys():
        if key not in id_col_list and not key.endswith("_src"):
            my_validation_logger.debug(f"Processing key: {key}")

            if mawnsrc_record[key] is None:
                mawnsrc_record[key + "_src"] = "EMPTY"
            else:
                if mawnsrc_record[key] == -7999:
                    mawnsrc_record[key] = None
                    mawnsrc_record[key + "_src"] = "OOR"
                else:
                    is_valid = check_value(key, mawnsrc_record[key], date_of_record)
                    if is_valid:
                        mawnsrc_record[key + "_src"] = default_source
                    elif key in relh_vars:
                        mawnsrc_record[key + "_src"] = "OOR"
                    else:
                        mawnsrc_record[key] = None
                        mawnsrc_record[key + "_src"] = "OOR"

    return mawnsrc_record



def relh_cap(mawnsrc_record: Dict[str, Any], relh_vars: List[str]) -> Dict[str, Any]:
    """
    Processes relative humidity values in the record, capping them at 100 and handling invalid values.

    Parameters:
        mawnsrc_record (Dict[str, Any]): The MAWN source record, where keys are strings 
                                        and values can be of any type.
        relh_vars (List[str]): List of relative humidity variable names to process.

    Returns:
        Dict[str, Any]: The processed MAWN source record with updated relative humidity values.

    Raises:
        ValueError: If mawnsrc_record or relh_vars is invalid.
    """
    my_validation_logger.info("Processing relative humidity values")

    if not isinstance(mawnsrc_record, dict) or not relh_vars:
        my_validation_logger.error("Invalid input parameters")
        raise ValueError("Valid mawnsrc_record and relh_vars are required")

    for key in mawnsrc_record:
        if key in relh_vars:
            my_validation_logger.debug(f"Processing RH key: {key}")
            current_value = mawnsrc_record[key]

            if current_value is None or mawnsrc_record.get(key + "_src") == "EMPTY":
                mawnsrc_record[key] = None
                my_validation_logger.debug(f"{key}: Set to None (empty/null value)")

            elif current_value == -7999:
                mawnsrc_record[key] = None
                mawnsrc_record[key + "_src"] = "EMPTY"
                my_validation_logger.debug(f"{key}: Invalid value (-7999), marked as EMPTY")

            elif 100 < current_value <= 105:
                mawnsrc_record[key + "_src"] = "RELH_CAP"
                mawnsrc_record[key] = 100
                my_validation_logger.debug(f"{key}: Value {current_value} capped at 100")

            elif current_value > 105:
                mawnsrc_record[key + "_src"] = "OOR"
                mawnsrc_record[key] = None
                my_validation_logger.debug(f"{key}: Value {current_value} out of range (>105)")

            elif current_value < 0:
                mawnsrc_record[key + "_src"] = "EMPTY"
                mawnsrc_record[key] = None
                my_validation_logger.debug(f"{key}: Negative value {current_value} set to None")

    my_validation_logger.info("Completed relative humidity processing")
    return mawnsrc_record


def replace_none_with_manwqc_record(
        mawnsrc_record: Dict[str, Any],
        mawnqc_record: Dict[str, Any],
        qc_columns: List[str]
) -> Dict[str, Any]:
    """
    Replace None values in the MAWN source record with calculated min/max or sum values from
    the MAWNQC record. Handles the special case for 'pcpn' (precipitation) and 'rpet' (evapotranspiration).

    Parameters:
        mawnsrc_record (Dict[str, Any]): MAWN source record.
        mawnqc_record (Dict[str, Any]): MAWNQC record with hourly data for value replacement.
        qc_columns (List[str]): Keys for quality control checks
   
    Returns:
        Dict[str, Any]: Updated MAWN source record with min/max or sum values where applicable.
    """
    my_validation_logger.info(f"Starting MAWNQC replacement")

    clean_record = mawnsrc_record.copy()
    my_validation_logger.debug("Created copy of MAWNQC record")

    # Adding any missing keys from qc_columns
    for key in qc_columns:
        if key not in clean_record:
            if key.endswith("_src"):
                clean_record[key] = "EMPTY"
                my_validation_logger.debug(f"Added missing source key {key} as EMPTY")
            else:
                clean_record[key] = None
                my_validation_logger.debug(f"Added missing data key {key} as None")
   
    # Performing MAWNQC value replacement
    for key in qc_columns:
        if key.endswith("_src"):
            data_key = key[:-4]  # Removing '_src' to get the data column name

            # Special case for 'pcpn' or 'rpet': calculate sum of hourly values
            if data_key == "pcpn" or data_key == "rpet":
                # Collect hourly precipitation values for the specific day
                hourly_values = [
                    mawnqc_record.get("pcpn") or mawnqc_record.get("rpet") for i in range(1, 25)
                    if mawnqc_record.get("hour") == i  # Ensure the hour matches
                ]
                if len(hourly_values) == 24:  # Ensure all hourly values are present
                    # Calculate the sum of all 24 hourly values for precipitation
                    sum_pcpn = sum(hourly_values)
                    clean_record[data_key] = sum_pcpn
                    clean_record[key + "_src"] = "MAWNQC"
                    my_validation_logger.debug(f"Replaced {data_key} with sum of hourly values from MAWNQC")
                else:
                    clean_record[key + "_src"] = "EMPTY"
                    my_validation_logger.warning(f"Missing hourly values for {data_key} in MAWNQC record")

            else:
                # General case for other variables
                hourly_values = [
                    mawnqc_record.get(data_key) for i in range(1, 25)
                    if mawnqc_record.get("hour") == i  # Matching the hours
                ]
                if len(hourly_values) == 24:  # Ensuring all hourly values are present
                    # Calculate min and max from the 24 hourly values
                    min_value = min(hourly_values)
                    max_value = max(hourly_values)

                    # Update the min and max values in the clean_records
                    if f"{data_key}_min" in clean_record:
                        clean_record[f"{data_key}_min"] = min_value  # Replace with min value
                        clean_record[key + "_src"] = "MAWNQC"  # Set source to MAWNQC
                        my_validation_logger.debug(f"Replaced {data_key}_min with min value from MAWNQC")

                    if f"{data_key}_max" in clean_record:
                        clean_record[f"{data_key}_max"] = max_value
                        clean_record[key + "_src"] = "MAWNQC"
                        my_validation_logger.debug(f"Replaced {data_key}_max with max value from MAWNQC")
                else:
                    my_validation_logger.warning(f"Hourly values not found for {data_key} in MAWNQC record")
                    clean_record[key + "_src"] = "EMPTY"  # Mark source as EMPTY

    # Final pass to ensure all _src keys are marked correctly
    for key in qc_columns:
        if key.endswith("_src"):
            data_key = key[:-4]
            if clean_record.get(key) is None:
                clean_record[key + "_src"] = "EMPTY"
                my_validation_logger.debug(f"Marked missing source {key} as EMPTY")
            if data_key not in clean_record or clean_record[data_key] is None:
                clean_record[key] = None
                my_validation_logger.debug(f"Ensured {data_key} remains None")
                           
    my_validation_logger.info("Completed MAWNQC value replacement")
    return clean_record



def filter_clean_record(clean_record: Dict[str, Any], qc_columns: List[str]) -> Dict[str, Any]:
    """
    Filter the clean record to retain specified quality control columns.
    Returns a new dictionary containing only the keys from the clean record 
    that are listed in qc_columns.

    Parameters:
    clean_record : Dict[str, Any]
        The clean record data.
    qc_columns : List[str]
        Keys to retain.

    Returns:
    Dict[str, Any]
        A dictionary with the specified quality control columns.
    """
    return {key: clean_record[key] for key in qc_columns if key in clean_record}


def inserting_empty_records(
        mawndbsrc_record: Dict[str, Any],
        mawnqc_record: Dict[str, Any],
        reference_date: date,
        qc_columns: List[str],
        id_col_list: List[str]
) -> Dict[str, Any]:
    """
    Inserts an empty record when no data is available.
    """
    _, _, date_obj = extract_record_components(mawndbsrc_record or mawnqc_record)

    day_of_year = date_obj.timetuple().tm_yday if date_obj else reference_date.timetuple().tm_yday

    if not day_of_year:
        my_validation_logger.error("Failed to determine day_of_year for empty record")
        return {}

    my_validation_logger.info(f"Creating empty record for day {day_of_year}")
    empty_record = {"day_of_year": day_of_year}

    for key in qc_columns:
        if key not in id_col_list:
            if mawndbsrc_record.get(key) is None and mawnqc_record.get(key) is None:
                empty_record[key] = None
                empty_record[key + "_src"] = "EMPTY"

    return empty_record



def estimate_daily_values(hourly_records: List[Dict[str, Any]], qc_columns: List[str]) -> Dict[str, Any]:
    """
    Estimate daily values from 24 hourly records.

    Parameters:
        hourly_records (List[Dict[str, Any]]): List of 24 hourly records.
        qc_columns (List[str]): List of quality control column names.

    Returns:
        Dict[str, Any]: Estimated daily record.
    """
    daily_estimates = {}

    # Extract numeric variables from records
    for key in hourly_records[0].keys():
        if key in qc_columns:
            continue  # Skip QC columns

        values = [rec[key] for rec in hourly_records if rec.get(key) is not None]

        if not values:
            daily_estimates[key] = None  # No data available
            continue

        # Special handling for PCPN and RPET (sum instead of min/max)
        if key in ["pcpn", "rpet"]:
            daily_estimates[key] = sum(values)
        else:
            daily_estimates[f"{key}_min"] = min(values)
            daily_estimates[f"{key}_max"] = max(values)

    return daily_estimates

def one_mawndb_record(mawndb_records: list) -> list:
    """
    Converts a list of MAWN DB records into a list of dictionaries.
    Parameters:
        mawndb_records (list): List of mawndb records.
    Returns:
        list: List of dictionaries representing the mawndb records.
    """
    # Use dictionary comprehension to create new dictionaries
    return [{k: v for k, v in record.items()} for record in mawndb_records]


def process_records(
    qc_columns: List[str],
    mawndb_records: List[Dict[str, Any]],
    mawnqc_records: List[Dict[str, Any]],
    begin_date: str,
    end_date: str,
) -> List[Dict[str, Any]]:
    """
    Process and combine MAWN and MAWNQC records for a given date range.
    """
    my_validation_logger.info(f"Starting record processing for period: {begin_date} to {end_date}")

    clean_records = []
    date_list = generate_list_of_dates(begin_date, end_date)
    id_col_list = ["year", "day", "date", "id"]

    for dt in date_list:
        print(f"\nProcessing date: {dt.date()}")

        # Get the MAWNDB daily record (expecting 1 record per day)
        matching_mawn_record = next(
            (rec for rec in mawndb_records if rec.get("date") == dt.date()),
            None
        )

        # Collect all matching hourly MAWNQC records for that day
        matching_mawnqc_records = [
            rec for rec in mawnqc_records
            if rec.get("date") == dt.date()
        ]

        clean_record = None

        if matching_mawn_record:
            my_validation_logger.debug("Found matching MAWN daily record")

            mawnsrc_record = creating_mawnsrc_record(matching_mawn_record, id_col_list, dt.date(), "MAWN")
            mawnsrc_record = relh_cap(mawnsrc_record, relh_vars)

            if matching_mawnqc_records:
                estimated_record = estimate_daily_values(matching_mawnqc_records, qc_columns)
                mawnsrc_record = replace_none_with_manwqc_record(mawnsrc_record, estimated_record, qc_columns)

            clean_record = filter_clean_record(mawnsrc_record, qc_columns)
            clean_records.append(clean_record)
            my_validation_logger.debug("Processed MAWN + MAWNQC record")

        elif matching_mawnqc_records:
            my_validation_logger.debug("No MAWN record, estimating from MAWNQC")

            estimated_record = estimate_daily_values(matching_mawnqc_records, qc_columns)
            mawnsrc_record = creating_mawnsrc_record(estimated_record, id_col_list, dt.date(), "MAWNQC")
            mawnsrc_record = relh_cap(mawnsrc_record, relh_vars)

            clean_record = filter_clean_record(mawnsrc_record, qc_columns)
            clean_records.append(clean_record)
            my_validation_logger.debug("Processed estimated MAWNQC record")

        else:
            # No data at all — insert empty record
            my_validation_logger.warning("No MAWN or MAWNQC data, inserting empty record")
            empty_record = inserting_empty_records({}, {}, dt.date(), qc_columns, id_col_list)
            clean_records.append(empty_record)

    my_validation_logger.info(f"Completed processing {len(clean_records)} records")
    return clean_records



