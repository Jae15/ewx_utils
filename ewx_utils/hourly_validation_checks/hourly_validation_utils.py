#!/usr/bin/python3
import os
import sys
from dotenv import load_dotenv
load_dotenv()
ewx_base_path = os.getenv("EWX_BASE_PATH")
sys.path.append(ewx_base_path)
from ewx_utils.ewx_config import ewx_log_file
import datetime
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from .hourly_variables_list import (
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
from typing import List, Dict, Any, Optional, Tuple
from .hourly_time_utils import generate_list_of_hours
from ewx_utils.logs.ewx_utils_logs_config import ewx_utils_logger

# Initialize the logger
my_validation_logger = ewx_utils_logger(log_path=ewx_log_file)


def check_value(k: str, v: float, d: datetime) -> bool:
    """
    Checks if a given value is valid based on its variable key and date.
    Parameters:
        k (str): The variable key.
        v (float): The value to check.
        d (datetime.datetime): The date of the value.
    Returns:
        bool: True if the value is valid, False otherwise.
    """
    if k in relh_vars:
        rh = Humidity(v, "PCT", d)
        return rh.is_in_range() and rh.is_valid()
    if k in pcpn_vars:
        pn = Precipitation(v, "hourly", "MM", d)
        return pn.is_valid()
    if k in rpet_vars:
        rp = Evapotranspiration(v, "hourly", "MM", d)
        return rp.is_valid()
    if k in temp_vars:
        tp = Temperature(v, "C", d)
        return tp.is_valid()
    if k in wspd_vars:
        ws = WindSpeed(v, "MPS", d)
        #print(k, v, d, ws.is_valid())
        return ws.is_valid()
    if k in wdir_vars:
        wd = WindDirection(v, "DEGREES", d)
        # print(k, v, d, tp.is_valid())
        return wd.is_valid()
    if k in leafwt_vars:
        lw = LeafWetness(v, "hourly", k, d)
        return lw.is_valid()
    if k in dwpt_vars:
        dp = Temperature(v, "C", d)
        return dp.is_valid()
    if k in vapr_vars:
        vp = VaporPressure(v, "hourly", d)
        return vp.is_valid()
    if k in mstr_vars:
        ms = SoilMoisture(v, d)
        return ms.is_valid()
    if k in nrad_vars:
        nr = NetRadiation(v, d)
        return nr.is_valid()
    if k in srad_vars:
        sr = SolarRadiation(v, d)
        return sr.is_valid()
    if k in sflux_vars:
        sf = SoilHeatFlux(v, d)
        return sf.is_valid()
    if k in wstdv_vars:
        wv = StdDevWindDirection(v, d)
        return wv.is_valid()
    if k in volt_vars:
        vt = Voltage(v, d)
        return vt.is_valid()
    return False


def combined_datetime(record: dict) -> datetime:
    """
    Combines the date and time from a record into a single datetime object.
    Parameters:
        record (dict): The record containing date and time.
    Returns:
        datetime.datetime: The combined datetime object, or None if date or time is missing.
    """
    date_value = record.get("date")
    time_value = record.get("time")
    if date_value and time_value:
        return datetime.combine(date_value, time_value)
    return None


def month_abbrv(record: Dict[str, str]) -> str:
    """
    Extracts the abbreviated month from a record's combined datetime.
    Parameters:
        record (dict): The record containing date and time.
    Returns:
        str: The abbreviated month, or None if the combined datetime is not available.
    """
    combined_date = combined_datetime(record)
    if combined_date:
        return combined_date.strftime("%b")
    return None


# Initialize lists for records and columns
record_keys = []
record_vals = []
db_columns = []
qc_values = []
clean_records = []


def getYearDayHour(combined_date: datetime) -> Optional[Dict[str, int]]:
    """
    Extracts year, day of the year, hour, and formatted reporting time from a given datetime object.

    Parameters:
        combined_date (datetime): A datetime object representing the date and time to be processed.

    Returns:
        Optional[Dict[str, int]]: A dictionary containing:
            - year: The year as integer
            - day: Day of the year as integer
            - hour: Hour as integer
            - rpt_time: Reporting time as string
            Returns None if an error occurs during processing.

    Raises:
        ValueError: If combined_date is None or invalid datetime object.
    """
    my_validation_logger.info(f"Processing datetime fields for: {combined_date}")

    # Initialize fields dictionary
    required_id_fields = {
        'year': None,
        'day': None,
        'hour': None,
        'rpt_time': None
    }

    try:
        # Set the timezone: combined_date is a naive datetime
        combined_date = combined_date.replace(tzinfo=ZoneInfo("America/Detroit"))
        my_validation_logger.debug(f"Set timezone to America/Detroit: {combined_date}")

        MIDNIGHT = datetime.strptime("00:00", '%H:%M').time()
        if combined_date.time() == MIDNIGHT:
            represented_date = combined_date.date() + timedelta(days=-1)
            hour = 24
            my_validation_logger.debug("Midnight case: adjusted date and set hour to 24")
        else:
            represented_date = combined_date.date()
            hour = combined_date.hour
            my_validation_logger.debug(f"Non-midnight case: using hour {hour}")

        required_id_fields['year'] = represented_date.year
        required_id_fields['day'] = represented_date.timetuple().tm_yday
        required_id_fields['hour'] = hour
        required_id_fields['rpt_time'] = str(hour) + '00'
        
        my_validation_logger.debug(
            f"Fields extracted - Year: {required_id_fields['year']}, "
            f"Day: {required_id_fields['day']}, Hour: {required_id_fields['hour']}"
        )

    except Exception as e:
        my_validation_logger.error(f"Error processing datetime {combined_date}: {str(e)}")
        return None

    my_validation_logger.info("Successfully processed datetime fields")
    return required_id_fields


def creating_mawnsrc_record(
    record: Dict[str, Any],
    combined_datetime: datetime,
    id_col_list: List[str],
    default_source: str,
) -> Dict[str, Any]:
    """
    Create a MAWN source record with appropriate source indicators.
    Checks values and sets source indicators based on conditions.

    Parameters:
        record (Dict[str, Any]): The original record to process.
        combined_datetime (datetime): The combined datetime for validation.
        id_col_list (List[str]): List of ID columns to exclude from validation.
        default_source (str): Default source indicator to use.

    Returns:
        Dict[str, Any]: The MAWN source record with source indicators.

    Raises:
        ValueError: If required parameters are missing or invalid.
    """
    my_validation_logger.info(f"Creating MAWN source record for {combined_datetime}")

    if not record or not id_col_list:
        my_validation_logger.error("Missing required record or id_col_list")
        raise ValueError("Record and id_col_list are required")

    mawnsrc_record = record.copy()
    my_validation_logger.debug("Created copy of original record")

    for key in record.keys():
        if key not in id_col_list and not key.endswith("_src"):
            my_validation_logger.debug(f"Processing key: {key}")

            if mawnsrc_record[key] is None:
                mawnsrc_record[key + "_src"] = "EMPTY"
                my_validation_logger.debug(f"{key}: Marked as EMPTY (None value)")
            else:
                if mawnsrc_record[key] == -7999:
                    mawnsrc_record[key] = None
                    mawnsrc_record[key + "_src"] = "OOR"
                    my_validation_logger.debug(f"{key}: Marked as OOR (-7999 value)")
                else:
                    value_check = check_value(key, mawnsrc_record[key], combined_datetime)
                    if value_check is True:
                        mawnsrc_record[key + "_src"] = default_source
                        my_validation_logger.debug(f"{key}: Validation passed, using {default_source}")
                    elif key in relh_vars:
                        mawnsrc_record[key + "_src"] = "OOR"
                        my_validation_logger.debug(f"{key}: Relative humidity OOR")
                    else:
                        mawnsrc_record[key] = None
                        mawnsrc_record[key + "_src"] = "OOR"
                        my_validation_logger.debug(f"{key}: Failed validation, marked as OOR")

    my_validation_logger.info("Completed MAWN source record creation")
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


def replace_none_with_rtmarecord(
    mawnsrc_record: Dict[str, Any],
    rtma_record: Dict[str, Any],
    combined_datetime: datetime,
    qc_columns: List[str],
) -> Dict[str, Any]:
    """
    Replace None values in the MAWN source record with values from the RTMA record.

    Parameters:
        mawnsrc_record (Dict[str, Any]): MAWN source record.
        rtma_record (Dict[str, Any]): RTMA record for value replacement.
        combined_datetime (datetime): Timestamp for the data.
        qc_columns (List[str]): Keys for quality control checks.

    Returns:
        Dict[str, Any]: Updated MAWN source record with RTMA values where applicable.

    Raises:
        ValueError: If required parameters are missing or invalid.
    """
    my_validation_logger.info(f"Starting RTMA replacement for {combined_datetime}")

    # Starting with a copy of the MAWN source record
    clean_record = mawnsrc_record.copy()
    my_validation_logger.debug("Created copy of MAWN source record")

    # Adding any missing keys from qc_columns
    for key in qc_columns:
        if key not in clean_record:
            if key.endswith("_src"):
                clean_record[key] = "EMPTY"  # Setting source keys to EMPTY if missing
                my_validation_logger.debug(f"Added missing source key {key} as EMPTY")
            else:
                clean_record[key] = None  # Setting data keys to None if missing
                my_validation_logger.debug(f"Added missing data key {key} as None")

    # Performing RTMA value replacement
    for key in qc_columns:
        if key.endswith("_src"):
            data_key = key[:-4]  # Extract the corresponding data key
            if data_key in rtma_record:
                # Check if the data key is None in clean_record and has a value in rtma_record
                if (
                    clean_record.get(data_key) is None
                    and rtma_record.get(data_key) is not None
                ):
                    my_validation_logger.debug(f"Found RTMA value for {data_key}")
                    # Validate the RTMA value before replacing
                    if check_value(data_key, rtma_record[data_key], combined_datetime):
                        clean_record[data_key] = rtma_record[data_key]  # Replace with RTMA value
                        clean_record[key] = "RTMA"  # Mark source as RTMA
                        my_validation_logger.debug(f"Replaced {data_key} with RTMA value")
                    else:
                        clean_record[key] = "EMPTY"  # Mark as EMPTY if value fails check
                        my_validation_logger.debug(f"RTMA value failed validation for {data_key}")
            else:
                # If the data key is None and RTMA key is missing, mark as EMPTY
                if clean_record.get(data_key) is None:
                    clean_record[key] = "EMPTY"
                    my_validation_logger.debug(f"No RTMA value found for {data_key}")

    # Final pass to ensure all _src keys are marked correctly
    for key in qc_columns:
        if key.endswith("_src"):
            data_key = key[:-4]
            if clean_record.get(key) is None:
                clean_record[key] = "EMPTY"  # Explicitly mark missing _src keys as EMPTY
                my_validation_logger.debug(f"Marked missing source {key} as EMPTY")
            if data_key not in clean_record or clean_record[data_key] is None:
                clean_record[data_key] = None  # Ensure missing data keys remain None
                my_validation_logger.debug(f"Ensured {data_key} remains None")

    my_validation_logger.info("Completed RTMA replacement process")
    return clean_record


def create_mawn_dwpt(mawnsrc_record: Dict[str, Any], combined_datetime: datetime) -> Dict[str, Any]:
    """
    Update the MAWN source record with dew point value if conditions are met.
    Calculates the dew point using temperature and humidity data if missing.

    Parameters:
        mawnsrc_record (Dict[str, Any]): Record containing temperature and humidity data.
        combined_datetime (datetime): Timestamp for the data.

    Returns:
        Dict[str, Any]: Updated record with dew point and source information.

    Raises:
        ValueError: If mawnsrc_record or combined_datetime is invalid.
    """
    my_validation_logger.info(f"Processing MAWN dew point for {combined_datetime}")

    if "dwpt" in mawnsrc_record.keys() and mawnsrc_record["dwpt"] is None:
        my_validation_logger.debug("Dew point is None, checking temperature and humidity")

        temp = (
            Temperature(mawnsrc_record["atmp"], "C", combined_datetime)
            if mawnsrc_record["atmp"] is not None
            else None
        )
        if temp is None:
            my_validation_logger.debug("Temperature value is None or invalid")
        else:
            my_validation_logger.debug(f"Temperature value: {mawnsrc_record['atmp']}°C")

        relh = (
            Humidity(mawnsrc_record["relh"], "PCT", combined_datetime)
            if mawnsrc_record["relh"] is not None
            else None
        )
        if relh is None:
            my_validation_logger.debug("Humidity value is None or invalid")
        else:
            my_validation_logger.debug(f"Humidity value: {mawnsrc_record['relh']}%")

        if temp is not None and relh is not None:
            try:
                dwpt_value = DewPoint(temp, relh, combined_datetime)
                mawnsrc_record["dwpt"] = dwpt_value.dwptC
                mawnsrc_record["dwpt_src"] = "MAWN"
                my_validation_logger.info(f"Calculated dew point: {dwpt_value.dwptC}°C")
            except Exception as e:
                my_validation_logger.error(f"Error calculating dew point: {str(e)}")
                mawnsrc_record["dwpt_src"] = "EMPTY"
        else:
            my_validation_logger.debug("Missing temperature or humidity for dew point calculation")
            mawnsrc_record["dwpt_src"] = "EMPTY"

    return mawnsrc_record


def create_rtma_dwpt(rtma_record: Dict[str, Any], combined_datetime: datetime) -> Dict[str, Any]:
    """
    Update the RTMA record with dew point value if conditions are met.
    If the dew point is missing, it calculates it using temperature and humidity data.

    Parameters:
        rtma_record (Dict[str, Any]): Record containing temperature and humidity data.
        combined_datetime (datetime): Timestamp for the data.

    Returns:
        Dict[str, Any]: Updated record with dew point and source information.

    Raises:
        ValueError: If rtma_record or combined_datetime is invalid.
    """
    my_validation_logger.info(f"Processing RTMA dew point for {combined_datetime}")

    if "dwpt" in rtma_record.keys() and rtma_record["dwpt"] is None:
        my_validation_logger.debug("Dew point is None, checking temperature and humidity")

        temp = (
            Temperature(rtma_record["atmp"], "C", combined_datetime)
            if rtma_record["atmp"] is not None
            else None
        )
        if temp is None:
            my_validation_logger.debug("Temperature value is None or invalid")
        else:
            my_validation_logger.debug(f"Temperature value: {rtma_record['atmp']}°C")

        relh = (
            Humidity(rtma_record["relh"], "PCT", combined_datetime)
            if rtma_record["relh"] is not None
            else None
        )
        if relh is None:
            my_validation_logger.debug("Humidity value is None or invalid")
        else:
            my_validation_logger.debug(f"Humidity value: {rtma_record['relh']}%")

        if temp is not None and relh is not None:
            try:
                dwpt_value = DewPoint(temp, relh, combined_datetime)
                rtma_record["dwpt"] = dwpt_value.dwptC
                rtma_record["dwpt_src"] = "RTMA"
                my_validation_logger.info(f"Calculated dew point: {dwpt_value.dwptC}°C")
            except Exception as e:
                my_validation_logger.error(f"Error calculating dew point: {str(e)}")
                rtma_record["dwpt"] = None
                rtma_record["dwpt_src"] = "EMPTY"
        else:
            my_validation_logger.debug("Missing temperature or humidity for dew point calculation")
            rtma_record["dwpt"] = None
            rtma_record["dwpt_src"] = "EMPTY"

    return rtma_record

def one_mawndb_record(mawndb_records: list) -> list:
    """
    Converts a list of MAWN DB records into a list of dictionaries.
    Parameters:
        mawndb_records (list): List of mawndb records.
    Returns:
        list: List of dictionaries representing the mawndb records.
    """
    return [dict(record) for record in mawndb_records]


def one_rtma_record(rtma_records: list) -> list:
    """
    Converts a list of RTMA records into a list of dictionaries.
    Parameters:
        rtma_records (list): List of RTMA records.
    Returns:
        list: List of dictionaries representing the RTMA records.
    """
    # print([dict(rtma_record) for rtma_record in rtma_records])
    return [dict(rtma_record) for rtma_record in rtma_records]


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
    rtma_record: Dict[str, Any],
    combined_date: datetime,
    qc_columns: List[str],
    id_col_list: List[str]
) -> Dict[str, Any]:
    """
    Inserts empty records when both the mawn record and the rtma record are none.

    Parameters:
        mawndbsrc_record (Dict[str, Any]): A dictionary representing a record from the MAWN database.
        rtma_record (Dict[str, Any]): A dictionary representing a record from the RTMA database.
        combined_date (datetime): A datetime object used to extract year, day, hour, and reporting time.
        qc_columns (List[str]): A list of keys that are considered quality control columns.
        id_col_list (List[str]): A list of keys that are considered identifier columns.

    Returns:
        Dict[str, Any]: A dictionary containing the processed data, including date, time,
                      year, day, hour, reporting time, and source indicators.

    Raises:
        ValueError: If required parameters are invalid or missing.
    """
    my_validation_logger.info(f"Creating empty record for {combined_date}")

    empty_record = {"date": combined_date.date(), "time": combined_date.time()}
    my_validation_logger.debug(f"Initialized empty record with date/time: {combined_date}")

    # Get year, day, hour, and rpt_time
    datetime_fields = getYearDayHour(combined_date)
    if datetime_fields:
        empty_record.update(datetime_fields)
        my_validation_logger.debug("Added datetime fields to empty record")
    else:
        my_validation_logger.warning("Failed to get datetime fields")

    # Check qc_columns for None in both records and set "_src" to "EMPTY"
    for key in qc_columns:
        if key not in id_col_list:
            if mawndbsrc_record.get(key) is None and rtma_record.get(key) is None:
                empty_record[key] = None
                if "_src" in key:
                    empty_record[key] = "EMPTY"
                    my_validation_logger.debug(f"Set source indicator for QC column: {key}")

    # Add source columns for keys in mawndbsrc_record with None values in both records
    for key in mawndbsrc_record.keys():
        if key not in qc_columns and key not in id_col_list:  
            if mawndbsrc_record.get(key) is None and rtma_record.get(key) is None:
                empty_record[key] = None
                if "_src" in key:
                    empty_record[key] = "EMPTY"
                    my_validation_logger.debug(f"Set source indicator for additional key: {key}")

    my_validation_logger.info("Completed empty record creation")
    return empty_record

def process_records(
    qc_columns: List[str],
    mawndb_records: List[Dict[str, Any]],
    rtma_records: List[Dict[str, Any]],
    begin_date: str,
    end_date: str,
) -> List[Dict[str, Any]]:
    """
    Process and combine MAWN and RTMA records for a given date range.

    Parameters:
        qc_columns (List[str]): List of quality control column names.
        mawndb_records (List[Dict[str, Any]]): List of MAWN database records.
        rtma_records (List[Dict[str, Any]]): List of RTMA records.
        begin_date (str): Start date of the processing period.
        end_date (str): End date of the processing period.

    Returns:
        List[Dict[str, Any]]: List of processed and cleaned records.

    Raises:
        ValueError: If required parameters are invalid or missing.
    """
    my_validation_logger.info(f"Starting record processing for period: {begin_date} to {end_date}")

    clean_records = []
    datetime_list = generate_list_of_hours(begin_date, end_date)
    id_col_list = ["year", "day", "hour", "rpt_time", "date", "time", "id"]  

    my_validation_logger.debug(f"Processing {len(datetime_list)} time periods")

    for dt in datetime_list:
        my_validation_logger.debug(f"Processing datetime: {dt}")
        matching_mawn_record = None
        matching_rtma_record = None
        clean_record = None

        # Process MAWN record if found
        for record in mawndb_records:
            if record["date"] == dt.date() and record["time"] == dt.time():
                matching_mawn_record = record
                my_validation_logger.debug(f"Found matching MAWN record for {dt}")
                break

        if matching_mawn_record:
            my_validation_logger.debug("Processing MAWN record")
            combined_date = combined_datetime(matching_mawn_record)
            mawnsrc_record = creating_mawnsrc_record(matching_mawn_record, combined_date, id_col_list, "MAWN")
            mawnsrc_record = relh_cap(mawnsrc_record, relh_vars)
            mawnsrc_record = create_mawn_dwpt(mawnsrc_record, combined_date)

            # Check for matching RTMA record
            for rtma_record in rtma_records:
                if rtma_record["date"] == dt.date() and rtma_record["time"] == dt.time():
                    matching_rtma_record = rtma_record
                    my_validation_logger.debug(f"Found matching RTMA record for {dt}")
                    combined_rtma_date = combined_datetime(rtma_record)
                    rtma_record = create_rtma_dwpt(rtma_record, combined_rtma_date)

                    clean_record = replace_none_with_rtmarecord(mawnsrc_record, rtma_record, combined_date, qc_columns)
                    clean_record = filter_clean_record(clean_record, qc_columns)
                    clean_records.append(clean_record)
                    my_validation_logger.debug("Processed MAWN+RTMA record combination")
                    break

            if not matching_rtma_record:
                my_validation_logger.debug("No matching RTMA record, using MAWN record only")
                clean_record = filter_clean_record(mawnsrc_record, qc_columns)
                clean_records.append(clean_record)
        else:
            # If no MAWN record, check for RTMA record
            for rtma_record in rtma_records:
                if rtma_record["date"] == dt.date() and rtma_record["time"] == dt.time():
                    matching_rtma_record = rtma_record
                    my_validation_logger.debug(f"Found RTMA record only for {dt}")
                    combined_rtma_date = combined_datetime(rtma_record)
                    rtma_record = create_rtma_dwpt(rtma_record, combined_rtma_date)

                    mawnsrc_record = creating_mawnsrc_record(rtma_record, combined_rtma_date, id_col_list, "RTMA")
                    mawnsrc_record = relh_cap(mawnsrc_record, relh_vars)
                    clean_record = replace_none_with_rtmarecord(mawnsrc_record, rtma_record, combined_rtma_date, qc_columns)
                    clean_record = filter_clean_record(clean_record, qc_columns)
                    clean_records.append(clean_record)
                    my_validation_logger.debug("Processed RTMA record")
                    break

        # If no matching records were found, create an empty record
        if not clean_record:
            my_validation_logger.debug(f"No matching records found for {dt}, creating empty record")
            combined_date = dt  
            empty_record = inserting_empty_records({}, {}, combined_date, qc_columns, id_col_list)
            clean_records.append(empty_record)

    my_validation_logger.info(f"Completed processing {len(clean_records)} records")
    return clean_records


