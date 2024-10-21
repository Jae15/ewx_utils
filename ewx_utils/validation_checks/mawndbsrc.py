#!/usr/bin/python3
import sys
import math
sys.path.append("c:/Users/mwangija/data_file/ewx_utils/ewx_utils")
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
    volt_vars
)
from mawndb_classes.humidity import Humidity
from mawndb_classes.dew_point import DewPoint
from mawndb_classes.windspeed import WindSpeed
from mawndb_classes.leafwetness import LeafWetness
from mawndb_classes.temperature import Temperature
from mawndb_classes.precipitation import Precipitation
from mawndb_classes.winddirection import WindDirection
from mawndb_classes.evapotranspiration import Evapotranspiration
from mawndb_classes.dew_point import DewPoint
from mawndb_classes.vapor_pressure import VaporPressure
from mawndb_classes.solar_radiation import SolarRadiation
from mawndb_classes.soil_moisture import SoilMoisture
from mawndb_classes.net_radiation import NetRadiation
from mawndb_classes.soil_heat_flux import SoilHeatFlux
from mawndb_classes.std_dev_wind_direction import StdDevWindDirection
from mawndb_classes.voltage import Voltage
from typing import List, Dict, Tuple
from .validation_logsconfig import validations_logger
from .timeloop import generate_list_of_hours

# Initialize the logger
my_validation_logger = validations_logger()

def getValueInSigFigs(num, sig_figs):
    if num == 0:
        return 0
    else:
        sig_figs = 6
        return round(num, sig_figs - int(math.floor(math.log10(abs(num)))) - 1)


def check_value(k: str, v: float, d: datetime.datetime) -> bool:
    """
    Checks if a given value is valid based on its variable key and date.
    Args:
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
        #print(pn.is_valid())
        #print(pn.pcpnMM)
        #print(pn.pcpnMM + pn.is_valid())

        return pn.is_valid()
    if k in rpet_vars:
        rp = Evapotranspiration(v, "hourly", "MM", d)
        return rp.is_valid()
    if k in temp_vars:
        tp = Temperature(v, "C", d)
        return tp.is_valid()
    if k in wspd_vars:
        ws = WindSpeed(v, "MPS", d)
        return ws.is_valid()
    if k in wdir_vars:
        wd = WindDirection(v, "DEGREES", d)
        return wd.is_valid()
    if k in leafwt_vars:
        lw = LeafWetness(v, "hourly", k, d)
        return lw.is_valid()
    if k in dwpt_vars:
        dp = Temperature(v,"C", d)
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
        v_sig_figs = getValueInSigFigs(v, 5)  # Apply rounding to v
        sr = SolarRadiation(v_sig_figs, d)  # Create an instance of SolarRadiation with rounded value
        return sr.is_valid()  # Return validity check
    if k in sflux_vars:
        sf = SoilHeatFlux(v,d)
        return sf.is_valid()
    if k in wstdv_vars:
        wv = StdDevWindDirection(v,d)
        return wv.is_valid()
    if k in volt_vars:
        vt = Voltage(v,d)
        return vt.is_valid()
    return False

def combined_datetime(record: dict) -> datetime.datetime:

    """
    Combines the date and time from a record into a single datetime object.
    Args:
        record (dict): The record containing date and time.
    Returns:
        datetime.datetime: The combined datetime object, or None if date or time is missing.
    """
    date_value = record.get("date")
    time_value = record.get("time")
    if date_value and time_value:
        return datetime.datetime.combine(date_value, time_value)
    return None


def month_abbrv(record: dict) -> str:
    """
    Extracts the abbreviated month from a record's combined datetime.
    Args:
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
    record: dict, combined_datetime: datetime.datetime, id_col_list: list
) -> dict:
    """
    Creates a MAWN source record by checking values and setting appropriate source indicators.
    Args:
        record (dict): The original record.
        combined_datetime (datetime.datetime): The combined datetime for validation.
        id_col_list (list): List of ID columns to exclude from validation.
    Returns:
        dict: The MAWN source record with source indicators.
    """
    mawnsrc_record = record.copy()
    for key in record.keys():
        if key not in id_col_list:
            if mawnsrc_record[key] is None:  # If there was no value originally
                mawnsrc_record[key] = None
                mawnsrc_record[key + "_src"] = "EMPTY"
            else:
                value_check = check_value(key, mawnsrc_record[key], combined_datetime)
                if value_check is True:
                    mawnsrc_record[key + "_src"] = "MAWN"
                elif key in relh_vars:  # and value_check was not True
                    mawnsrc_record[key + "_src"] = "OOR"
                else:
                    mawnsrc_record[key] = None
                    mawnsrc_record[key + "_src"] = "OOR"
    return mawnsrc_record


def relh_cap(mawnsrc_record: dict, relh_vars: list) -> dict:
    """
    Processes relative humidity values in the record, capping them at 100 and handling invalid values.
    Args:
        mawnsrc_record (dict): The MAWN source record.
        relh_vars (list): List of relative humidity variables.
    Returns:
        dict: The processed MAWN source record.
    """
    for key in mawnsrc_record:
        if key in relh_vars and (mawnsrc_record[key] is None or mawnsrc_record.get(key + "_src") == "EMPTY"):
            mawnsrc_record[key] = None
        elif key in relh_vars and 100 <= mawnsrc_record[key] <= 105:
            mawnsrc_record[key + "_src"] = "RELH_CAP"
            mawnsrc_record[key] = 100

    return mawnsrc_record

def create_rtma_dwpt(rtma_record: dict, combined_datetime: datetime.datetime) -> dict:
    if 'dwpt' in rtma_record.keys():
        #dwpt_key_rtma = rtma_record['dwpt']
        if rtma_record['dwpt'] is None:
            # get the atmp and use it to create a temp object
            # get the relh
            temp = Temperature(rtma_record['atmp'], 'C', combined_datetime)
            relh = Humidity(rtma_record['relh'], 'PCT',combined_datetime)
            dwpt = DewPoint(temp, relh, combined_datetime)
            dwptC = dwpt.calculate_dew_point()
            rtma_record['dwpt'] = dwptC
    return rtma_record

def replace_none_with_rtmarecord(
    mawnsrc_record: dict, rtma_record: dict, combined_datetime: datetime.datetime
) -> dict:
    """
    Replaces None values in a MAWN source record with corresponding values from an RTMA record.
    Args:
        mawnsrc_record (dict): The MAWN source record.
        rtma_record (dict): The RTMA record.
        combined_datetime (datetime.datetime): The combined datetime for validation.
    Returns:
        dict: The cleaned MAWN source record.
    """
    clean_record = mawnsrc_record.copy()
    for key in clean_record.keys():
        if key.endswith("_src"):
            data_key = key[:-4]
            if (
                mawnsrc_record.get(data_key) is None
                and rtma_record.get(data_key) is not None
            ):
                if check_value(data_key, rtma_record[data_key], combined_datetime):
                    clean_record[data_key] = rtma_record[data_key]
                    clean_record[key] = "RTMA"
                else:
                    clean_record[data_key] = None
                    clean_record[key] = "EMPTY"
    return clean_record


def one_mawndb_record(mawndb_records: list) -> list:
    """
    Converts a list of MAWN DB records into a list of dictionaries.
    Args:
        mawndb_records (list): List of mawndb records.
    Returns:
        list: List of dictionaries representing the mawndb records.
    """
    return [dict(record) for record in mawndb_records]


def one_rtma_record(rtma_records: list) -> list:
    """
    Converts a list of RTMA records into a list of dictionaries.
    Args:
        rtma_records (list): List of RTMA records.
    Returns:
        list: List of dictionaries representing the RTMA records.
    """
    return [dict(rtma_record) for rtma_record in rtma_records]

def process_records(mawndb_records: List[Dict], rtma_records: List[Dict], begin_date: str, end_date: str) -> List[Dict]:
    """
    Process and clean the records by utilizing the validation logic.

    Args:
    mawn_records: List of records from mawndb.
    rtma_records: List of records from rtma.
    begin_date: Start date for the records to process.
    end_date: End date for the records to process.

    Returns:
    List of cleaned records.
    """
    clean_records = []
    datetime_list = generate_list_of_hours(begin_date, end_date)

    # Process each hour in the datetime list
    for dt in datetime_list:
        matching_mawn_record = None
        matching_rtma_record = None

        # Find the matching MAWN record for the current datetime
        for record in mawndb_records:
            if record["date"] == dt.date() and record["time"] == dt.time():
                matching_mawn_record = record
                break

        # Process the MAWN record if found
        if matching_mawn_record:
            combined_date = combined_datetime(matching_mawn_record)
            id_col_list = ["year", "day", "hour", "rpt_time", "date", "time", "id"]

            # Create and validate the MAWN source record
            mawnsrc_record = creating_mawnsrc_record(matching_mawn_record, combined_date, id_col_list)
            mawnsrc_record = relh_cap(mawnsrc_record, relh_vars)

            # Find the matching RTMA record for the same datetime
            for rtma_record in rtma_records:
                if rtma_record["date"] == dt.date() and rtma_record["time"] == dt.time():
                    matching_rtma_record = rtma_record
                    combined_rtma_date = combined_datetime(rtma_record)
                    rtma_record = create_rtma_dwpt(rtma_record, combined_rtma_date)

                    # Replace None values in MAWN record with corresponding RTMA values
                    clean_record = replace_none_with_rtmarecord(mawnsrc_record, rtma_record, combined_date)
                    clean_records.append(clean_record)
                    break

            # If no matching RTMA record is found, keep the MAWN record as is
            if not matching_rtma_record:
                clean_records.append(mawnsrc_record)

        else:
            # If no MAWN record exists for this datetime, process RTMA records directly
            for rtma_record in rtma_records:
                if rtma_record["date"] == dt.date() and rtma_record["time"] == dt.time():
                    combined_rtma_date = combined_datetime(rtma_record)
                    rtma_record = create_rtma_dwpt(rtma_record, combined_rtma_date)

                    # Create a MAWN-like source record directly from the RTMA record
                    mawnsrc_record = creating_mawnsrc_record(rtma_record, combined_rtma_date, id_col_list=[])
                    mawnsrc_record = relh_cap(mawnsrc_record, relh_vars)
                    clean_records.append(mawnsrc_record)
                    break

    return clean_records

# Provide the functions needed for external scripts
__all__ = [
    "check_value",
    "combined_datetime",
    "creating_mawnsrc_record",
    "relh_cap",
    "create_rtma_dwpt",
    "replace_none_with_rtmarecord",
    "process_records"
]
