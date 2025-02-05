#!/usr/bin/python3
import os
import sys
from dotenv import load_dotenv

load_dotenv()
ewx_base_path = os.getenv("EWX_BASE_PATH")
sys.path.append(ewx_base_path)
from ewx_utils.ewx_config import ewx_log_file
import datetime
from datetime import timezone
from zoneinfo import ZoneInfo
from .variables_list import (
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
from typing import List, Dict, Any, Tuple
from .time_utils import generate_list_of_hours
from ewx_utils.logs.ewx_utils_logs_config import ewx_utils_logger

# Initialize the logger
my_validation_logger = ewx_utils_logger(log_path=ewx_log_file)


def check_value(k: str, v: float, d: datetime.datetime) -> bool:
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
        # print(k, v, d, tp.is_valid())
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


def combined_datetime(record: dict) -> datetime.datetime:
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
        return datetime.datetime.combine(date_value, time_value)
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

def creating_mawnsrc_record(
    record: Dict[str, Any],
    combined_datetime: datetime.datetime,
    id_col_list: List[str],
    default_source: str,
) -> Dict[str, Any]:
    """
    Create a MAWN source record with appropriate source indicators.
    Checks values and sets source indicators based on conditions.

    Parameters:
    record : Dict[str, Any]
        The original record.
    combined_datetime : datetime.datetime
        The combined datetime for validation.
    id_col_list : List[str]
        List of ID columns to exclude from validation.

    Returns:
    Dict[str, Any]
        The MAWN source record with source indicators.
    """
    mawnsrc_record = record.copy()
    for key in record.keys():
        if key not in id_col_list and not key.endswith("_src"):
            if mawnsrc_record[key] is None:
                mawnsrc_record[key + "_src"] = "EMPTY"
            else:
                if mawnsrc_record[key] == -7999:
                    mawnsrc_record[key] = None
                    mawnsrc_record[key + "_src"] = "OOR"
                else:
                    value_check = check_value(key, mawnsrc_record[key], combined_datetime)
                    if value_check is True:
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
    mawnsrc_record : Dict[str, Any]
        The MAWN source record, where keys are strings and values can be of any type.
    relh_vars : List[str]
        List of relative humidity variable names to process.

    Returns:
    Dict[str, Any]
        The processed MAWN source record with updated relative humidity values.
    """
    for key in mawnsrc_record:
        if key in relh_vars and (
            mawnsrc_record[key] is None or mawnsrc_record.get(key + "_src") == "EMPTY"
        ):
            mawnsrc_record[key] = None
        elif key in relh_vars and mawnsrc_record[key] == -7999:
            mawnsrc_record[key] = None
            mawnsrc_record[key + "_src"] = "EMPTY"
        elif key in relh_vars and 100 < mawnsrc_record[key] <= 105:
            mawnsrc_record[key + "_src"] = "RELH_CAP"
            mawnsrc_record[key] = 100
        elif key in relh_vars and mawnsrc_record[key] > 105:
            mawnsrc_record[key + "_src"] = "OOR"
            mawnsrc_record[key] = None
        elif key in relh_vars and mawnsrc_record[key] < 0:
            mawnsrc_record[key + "_src"] = "EMPTY"
            mawnsrc_record[key] = None

    return mawnsrc_record 

def replace_none_with_rtmarecord(
    mawnsrc_record: Dict[str, Any],
    rtma_record: Dict[str, Any],
    combined_datetime: datetime.datetime,
    qc_columns: List[str],
) -> Dict[str, Any]:
    """
    Replace None values in the MAWN source record with values from the RTMA record.

    Parameters:
    mawnsrc_record : Dict[str, Any]
        MAWN source record.
    rtma_record : Dict[str, Any]
        RTMA record for value replacement.
    combined_datetime : datetime.datetime
        Timestamp for the data.
    qc_columns : List[str]
        Keys for quality control checks.

    Returns:
    Dict[str, Any]
        Updated MAWN source record with RTMA values where applicable.
    """
    # Starting with a copy of the MAWN source record
    clean_record = mawnsrc_record.copy()

    # Adding any missing keys from qc_columns
    for key in qc_columns:
        if key not in clean_record:
            if key.endswith("_src"):
                clean_record[key] = "EMPTY"  # Setting source keys to EMPTY if missing
            else:
                clean_record[key] = None  # Setting data keys to None if missing

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
                    # Validate the RTMA value before replacing
                    if check_value(data_key, rtma_record[data_key], combined_datetime):
                        clean_record[data_key] = rtma_record[
                            data_key
                        ]  # Replace with RTMA value
                        clean_record[key] = "RTMA"  # Mark source as RTMA
                    else:
                        clean_record[key] = (
                            "EMPTY"  # Mark as EMPTY if value fails check
                        )
            else:
                # If the data key is None and RTMA key is missing, mark as EMPTY
                if clean_record.get(data_key) is None:
                    clean_record[key] = "EMPTY"

    # Final pass to ensure all _src keys are marked correctly
    for key in qc_columns:
        if key.endswith("_src"):
            data_key = key[:-4]
            if clean_record.get(key) is None:
                clean_record[key] = (
                    "EMPTY"  # Explicitly mark missing _src keys as EMPTY
                )
            if data_key not in clean_record or clean_record[data_key] is None:
                clean_record[data_key] = None  # Ensure missing data keys remain None

    return clean_record


def create_mawn_dwpt(mawnsrc_record: Dict[str, Any], combined_datetime: datetime.datetime) -> Dict[str, Any]:
    """
    Update the MAWN source record with dew point value if conditions are met.
    Calculates the dew point using temperature and humidity data if missing.

    Parameters:
    mawnsrc_record : Dict[str, Any]
        Record containing temperature and humidity data.
    combined_datetime : datetime.datetime
        Timestamp for the data.

    Returns:
    Dict[str, Any]
        Updated record with dew point and source information.
    """
    if "dwpt" in mawnsrc_record.keys() and mawnsrc_record["dwpt"] is None:
        temp = (
            Temperature(mawnsrc_record["atmp"], "C", combined_datetime)
            if mawnsrc_record["atmp"] is not None
            else None
        )
        relh = (
            Humidity(mawnsrc_record["relh"], "PCT", combined_datetime)
            if mawnsrc_record["relh"] is not None
            else None
        )

        if temp is not None and relh is not None:
            dwpt_value = DewPoint(temp, relh, combined_datetime)
            mawnsrc_record["dwpt"] = dwpt_value.dwptC
            mawnsrc_record["dwpt_src"] = "MAWN"
        else:
            mawnsrc_record["dwpt_src"] = "EMPTY"
    return mawnsrc_record


def create_rtma_dwpt(rtma_record: Dict[str, Any], combined_datetime: datetime.datetime) -> Dict[str, Any]:
    """
    Update the RTMA record with dew point value if conditions are met.
    If the dew point is missing, it calculates it using temperature and humidity data.

    Parameters:
    rtma_record : Dict[str, Any]
        Record containing temperature and humidity data.
    combined_datetime : datetime.datetime
        Timestamp for the data.

    Returns:
    Dict[str, Any]
        Updated record with dew point and source information.
    """
    if "dwpt" in rtma_record.keys() and rtma_record["dwpt"] is None:
        temp = (
            Temperature(rtma_record["atmp"], "C", combined_datetime)
            if rtma_record["atmp"] is not None
            else None
        )
        relh = (
            Humidity(rtma_record["relh"], "PCT", combined_datetime)
            if rtma_record["relh"] is not None
            else None
        )

        if temp is not None and relh is not None:
            dwpt_value = DewPoint(temp, relh, combined_datetime)
            rtma_record["dwpt"] = dwpt_value.dwptC
            rtma_record["dwpt_src"] = "RTMA"
        else:
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

def process_records(
    qc_columns: List[str],
    mawndb_records: List[Dict[str, Any]],
    rtma_records: List[Dict[str, Any]],
    begin_date: str,
    end_date: str,
) -> List[Dict[str, Any]]:
    """
    Process MAWN and RTMA records within a specified date range.

    This function matches MAWN and RTMA records based on date and time, 
    creating clean records by filtering and replacing missing values.

    Parameters:
    qc_columns : List[str]
        Keys for quality control filtering.
    mawndb_records : List[Dict[str, Any]]
        MAWN database records.
    rtma_records : List[Dict[str, Any]]
        RTMA records.
    begin_date : str
        Start date for processing.
    end_date : str
        End date for processing.

    Returns:
    List[Dict[str, Any]]
        A list of processed clean records.
    """
    clean_records = []
    datetime_list = generate_list_of_hours(begin_date, end_date)

    # Process each hour in the datetime list
    for dt in datetime_list:
        matching_mawn_record = None
        matching_rtma_record = None
        id_col_list = ["year", "day", "hour", "rpt_time", "date", "time", "id"]

        # Find the matching MAWN record for the current datetime
        for record in mawndb_records:
            if(
                record["date"] == dt.date() 
                and record["time"] == dt.time()
            ):
                matching_mawn_record = record
             
                break

        # Process the MAWN record if found
        if matching_mawn_record:
            combined_date = combined_datetime(matching_mawn_record)
            # Create and validate the MAWN source record
            mawnsrc_record = creating_mawnsrc_record(
                matching_mawn_record, combined_date, id_col_list, "MAWN"
            )
            mawnsrc_record = relh_cap(mawnsrc_record, relh_vars)
            mawnsrc_record = create_mawn_dwpt(mawnsrc_record, combined_date)

            # Find the matching RTMA record for the same datetime
            for rtma_record in rtma_records:
                if (
                    rtma_record["date"] == dt.date()
                    and rtma_record["time"] == dt.time()
                ):
                    matching_rtma_record = rtma_record
                    combined_rtma_date = combined_datetime(rtma_record)
                    rtma_record = create_rtma_dwpt(rtma_record, combined_rtma_date)

                    # Replace None values in MAWN record with corresponding RTMA values
                    clean_record = replace_none_with_rtmarecord(
                        mawnsrc_record, rtma_record, combined_date, qc_columns
                    )
                    clean_record = filter_clean_record(clean_record, qc_columns)
                    clean_records.append(clean_record)
                    #print(f"Clean Record: {clean_record}")
                    break

            # If no matching RTMA record is found, keep the MAWN record as is
            if not matching_rtma_record:
                clean_record = filter_clean_record(mawnsrc_record, qc_columns)
                #print(f"Clean Record: {clean_record}")
                clean_records.append(clean_record)

        else:
            # If no MAWN record exists for this datetime, process RTMA records directly
            for rtma_record in rtma_records:
                if (
                    rtma_record["date"] == dt.date()
                    and rtma_record["time"] == dt.time()
                ):
                    combined_rtma_date = combined_datetime(rtma_record)
                    rtma_record = create_rtma_dwpt(rtma_record, combined_rtma_date)

                    # Create a MAWN-like source record directly from the RTMA record
                    mawnsrc_record = creating_mawnsrc_record(
                        rtma_record, combined_rtma_date, id_col_list, "RTMA"
                    )
                    mawnsrc_record = relh_cap(mawnsrc_record, relh_vars)
                    clean_record = replace_none_with_rtmarecord(
                        mawnsrc_record, rtma_record, combined_date, qc_columns
                    )
                    clean_record = filter_clean_record(clean_record, qc_columns)
                    #print(f"Clean Record: {clean_record}")
                    clean_records.append(clean_record)
                    break

    return clean_records
