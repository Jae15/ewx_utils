#!/usr/bin/python3
import psycopg2
import logging
import sys

sys.path.append("c:/Users/mwangija/data_file/ewx_utils/ewx_utils")
import pprint
import psycopg2.extras
import datetime
from datetime import timezone
from zoneinfo import ZoneInfo
from variables_list import (
    relh_vars,
    pcpn_vars,
    rpet_vars,
    temp_vars,
    wspd_vars,
    wdir_vars,
    leafwt_vars,
    dwpt_vars
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
from typing import List, Dict, Tuple
from validation_logsconfig import validations_logger

# Initialize the logger
my_validation_logger = validations_logger()


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
    return True


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


def main():
    """
    Main function to process example records and perform validation and cleaning.
    
    # Example Records
    rtma_records = [
        {
            "year": 2022,
            "day": 68,
            "hour": 24,
            "rpt_time": 2400,
            "date": datetime.date(2022, 3, 10),
            "time": datetime.time(0, 0),
            "atmp": -4.92,
            "relh": 81.35,
            "dwpt": None,
            "pcpn": 0.0,
            "lws0_pwet": None,
            "lws1_pwet": None,
            "wspd": 0.938,
            "wdir": 291.6,
            "wspd_max": 6.897,
            "srad": None,
            "stmp_05cm": None,
            "stmp_10cm": None,
            "stmp_20cm": None,
            "stmp_50cm": None,
            "smst_05cm": None,
            "smst_10cm": None,
            "smst_20cm": None,
            "smst_50cm": None,
            "rpet": None,
            "id": 18720707,
        },
        {
            "year": 2022,
            "day": 69,
            "hour": 1,
            "rpt_time": 100,
            "date": datetime.date(2022, 3, 10),
            "time": datetime.time(1, 0),
            "atmp": -5.17,
            "relh": 79.35,
            "dwpt": None,
            "pcpn": 0.0,
            "lws0_pwet": None,
            "lws1_pwet": None,
            "wspd": 0.631,
            "wdir": 253.2,
            "wspd_max": 6.051,
            "srad": None,
            "stmp_05cm": None,
            "stmp_10cm": None,
            "stmp_20cm": None,
            "stmp_50cm": None,
            "smst_05cm": None,
            "smst_10cm": None,
            "smst_20cm": None,
            "smst_50cm": None,
            "rpet": None,
            "id": 18721453,
        },
    ]
    mawndb_records = [
        {
            "year": 2022,
            "day": 68,
            "hour": 24,
            "rpt_time": 2400,
            "date": datetime.date(2022, 3, 10),
            "time": datetime.time(0, 0),
            "atmp": 500,
            "relh": 500,
            "dwpt": 500,
            "pcpn": 500,
            "lws0_pwet": 500,
            "lws1_pwet": 500,
            "wspd": 500,
            "wdir": 500,
            "wspd_max": 500,
            "srad": 500,
            "stmp_05cm": 500,
            "stmp_10cm": 500,
            "stmp_20cm": 500,
            "stmp_50cm": 500,
            "smst_05cm": 500,
            "smst_10cm": 500,
            "smst_20cm": 500,
            "smst_50cm": 500,
            "rpet": 500,
            "id": 18720707,
        },
        {
            "year": 2022,
            "day": 69,
            "hour": 1,
            "rpt_time": 100,
            "date": datetime.date(2022, 3, 10),
            "time": datetime.time(1, 0),
            "atmp": -5.0,
            "relh": 79,
            "dwpt": -8.4,
            "pcpn": 0.0,
            "lws0_pwet": 0,
            "lws1_pwet": 0,
            "wspd": 0.7,
            "wdir": 253.0,
            "wspd_max": 5.8,
            "srad": 0.0,
            "stmp_05cm": 1.2,
            "stmp_10cm": 1.2,
            "stmp_20cm": 1.7,
            "stmp_50cm": 2.8,
            "smst_05cm": 0.273,
            "smst_10cm": 0.282,
            "smst_20cm": 0.291,
            "smst_50cm": 0.334,
            "rpet": 0.0,
            "id": 18721453,
        },
    ]
    """
    mawndb_records = [{'year': 2019, 'day': 230, 'hour': 12, 'rpt_time': 1200, 'date': datetime.date(2019, 8, 18), 'time': datetime.time(12, 0), 'atmp': None, 'relh': None, 'dwpt': None, 'pcpn': 11.43, 'lws0_pwet': 0.724, 'lws1_pwet': None, 'wspd': 3.527, 'wdir': 182.6, 'wstdv': 27.64, 'wspd_max': 14.2, 'wspd_maxt': 1123, 'srad': 109.1811, 'stmp_05cm': 21.81, 'stmp_10cm': 21.22, 'stmp_20cm': 20.87, 'stmp_50cm': 20.42, 'smst_05cm': 0.216, 'smst_10cm': 0.148, 'smst_20cm': 0.127, 'smst_50cm': 0.251, 'volt': 13.1, 'rpet': None, 'id': 7356}, 
                      {'year': 2019, 'day': 230, 'hour': 13, 'rpt_time': 1300, 'date': datetime.date(2019, 8, 18), 'time': datetime.time(13, 0), 'atmp': None, 'relh': None, 'dwpt': None, 'pcpn': 2.286, 'lws0_pwet': 1.0, 'lws1_pwet': None, 'wspd': 3.459, 'wdir': 140.5, 'wstdv': 17.55, 'wspd_max': 7.2, 'wspd_maxt': 1232, 'srad': 203.3628, 'stmp_05cm': 21.39, 'stmp_10cm': 21.29, 'stmp_20cm': 20.96, 'stmp_50cm': 20.4, 'smst_05cm': 0.26, 'smst_10cm': 0.15, 'smst_20cm': 0.127, 'smst_50cm': 0.251, 'volt': 13.18, 'rpet': None, 'id': 7357},
                      {'year': 2019, 'day': 230, 'hour': 14, 'rpt_time': 1400, 'date': datetime.date(2019, 8, 18), 'time': datetime.time(14, 0), 'atmp': None, 'relh': None, 'dwpt': None, 'pcpn': 0.0, 'lws0_pwet': 0.456, 'lws1_pwet': None, 'wspd': 4.169, 'wdir': 148.0, 'wstdv': 21.19, 'wspd_max': 7.7, 'wspd_maxt': 1345, 'srad': 1628.859, 'stmp_05cm': 21.22, 'stmp_10cm': 21.16, 'stmp_20cm': 20.99, 'stmp_50cm': 20.38, 'smst_05cm': 0.266, 'smst_10cm': 0.153, 'smst_20cm': 0.127, 'smst_50cm': 0.251, 'volt': 13.3, 'rpet': None, 'id': 7358}, 
                      {'year': 2019, 'day': 230, 'hour': 15, 'rpt_time': 1500, 'date': datetime.date(2019, 8, 18), 'time': datetime.time(15, 0), 'atmp': None, 'relh': None, 'dwpt': None, 'pcpn': 0.0, 'lws0_pwet': 0.0, 'lws1_pwet': None, 'wspd': 2.271, 'wdir': 195.8, 'wstdv': 29.08, 'wspd_max': 5.2, 'wspd_maxt': 1417, 'srad': 1986.219, 'stmp_05cm': 21.93, 'stmp_10cm': 21.25, 'stmp_20cm': 20.98, 'stmp_50cm': 20.36, 'smst_05cm': 0.264, 'smst_10cm': 0.156, 'smst_20cm': 0.127, 'smst_50cm': 0.25, 'volt': 13.2, 'rpet': None, 'id': 7359}, 
                      {'year': 2019, 'day': 230, 'hour': 16, 'rpt_time': 1600, 'date': datetime.date(2019, 8, 18), 'time': datetime.time(16, 0), 'atmp': None, 'relh': None, 'dwpt': None, 'pcpn': 0.0, 'lws0_pwet': 0.0, 'lws1_pwet': None, 'wspd': 3.282, 'wdir': 219.2, 'wstdv': 22.28, 'wspd_max': 8.7, 'wspd_maxt': 1553, 'srad': 1625.044, 'stmp_05cm': 22.79, 'stmp_10cm': 21.68, 'stmp_20cm': 21.03, 'stmp_50cm': 20.36, 'smst_05cm': 0.261, 'smst_10cm': 0.16, 'smst_20cm': 0.128, 'smst_50cm': 0.25, 'volt': 13.19, 'rpet': None, 'id': 7360}]
    rtma_records = [{'year': 2019, 'day': 230, 'hour': 12, 'rpt_time': 1200, 'date': datetime.date(2019, 8, 18), 'time': datetime.time(12, 0), 'atmp': 19.42, 'relh': 97.43, 'dwpt': 19.0, 'pcpn': 1.4, 'lws0_pwet': None, 'lws1_pwet': None, 'wspd': 1.461, 'wdir': 260.4, 'wspd_max': 7.777, 'srad': None, 'stmp_05cm': None, 'stmp_10cm': None, 'stmp_20cm': None, 'stmp_50cm': None, 'smst_05cm': None, 'smst_10cm': None, 'smst_20cm': None, 'smst_50cm': None, 'rpet': None, 'id': 5706}, 
                    {'year': 2019, 'day': 230, 'hour': 13, 'rpt_time': 1300, 'date': datetime.date(2019, 8, 18), 'time': datetime.time(13, 0), 'atmp': 19.89, 'relh': 96.24, 'dwpt': 19.27, 'pcpn': 0.0, 'lws0_pwet': None, 'lws1_pwet': None, 'wspd': 4.955, 'wdir': 169.5, 'wspd_max': 7.86, 'srad': None, 'stmp_05cm': None, 'stmp_10cm': None, 'stmp_20cm': None, 'stmp_50cm': None, 'smst_05cm': None, 'smst_10cm': None, 'smst_20cm': None, 'smst_50cm': None, 'rpet': None, 'id': 5708}, 
                    {'year': 2019, 'day': 230, 'hour': 14, 'rpt_time': 1400, 'date': datetime.date(2019, 8, 18), 'time': datetime.time(14, 0), 'atmp': 22.45, 'relh': 80.08, 'dwpt': 18.83, 'pcpn': 0.0, 'lws0_pwet': None, 'lws1_pwet': None, 'wspd': 2.897, 'wdir': 167.5, 'wspd_max': 8.383, 'srad': None, 'stmp_05cm': None, 'stmp_10cm': None, 'stmp_20cm': None, 'stmp_50cm': None, 'smst_05cm': None, 'smst_10cm': None, 'smst_20cm': None, 'smst_50cm': None, 'rpet': None, 'id': 5710}, 
                    {'year': 2019, 'day': 230, 'hour': 15, 'rpt_time': 1500, 'date': datetime.date(2019, 8, 18), 'time': datetime.time(15, 0), 'atmp': 24.92, 'relh': 68.84, 'dwpt': 18.79, 'pcpn': 0.0, 'lws0_pwet': None, 'lws1_pwet': None, 'wspd': 3.826, 'wdir': 231.2, 'wspd_max': 6.897, 'srad': None, 'stmp_05cm': None, 'stmp_10cm': None, 'stmp_20cm': None, 'stmp_50cm': None, 'smst_05cm': None, 'smst_10cm': None, 'smst_20cm': None, 'smst_50cm': None, 'rpet': None, 'id': 5712}, 
                    {'year': 2019, 'day': 230, 'hour': 16, 'rpt_time': 1600, 'date': datetime.date(2019, 8, 18), 'time': datetime.time(16, 0), 'atmp': 24.56, 'relh': 70.21, 'dwpt': 18.76, 'pcpn': 0.0, 'lws0_pwet': None, 'lws1_pwet': None, 'wspd': 6.474, 'wdir': 243.1, 'wspd_max': 10.74, 'srad': None, 'stmp_05cm': None, 'stmp_10cm': None, 'stmp_20cm': None, 'stmp_50cm': None, 'smst_05cm': None, 'smst_10cm': None, 'smst_20cm': None, 'smst_50cm': None, 'rpet': None, 'id': 5714}]

    # Iterate over records and perform validation and cleaning
    for record in mawndb_records:
        # Combine the date and time parts of the record to form a single datetime object
        combined_date = combined_datetime(record)

        # Define the list of ID columns to be used for creating the MAWN source record
        id_col_list = ["year", "day", "hour", "rpt_time", "date", "time", "id"]

        # Create the initial MAWN source record using the record data, combined datetime, and ID columns
        mawnsrc_record = creating_mawnsrc_record(record, combined_date, id_col_list)

        # Apply relative humidity cap to the MAWN source record using the relative humidity variables
        mawnsrc_record = relh_cap(mawnsrc_record, relh_vars)

        # Find the corresponding RTMA record by matching 'date', 'time', and 'hour'
        rtma_record = None
        for rt in rtma_records:
            if (
                rt["date"] == record["date"]
                and rt["time"] == record["time"]
                and rt["hour"] == record["hour"]
            ):
                rtma_record = rt
                break

        # If a matching RTMA record is found, replace None values in the MAWN source record with valid values from the RTMA record
        if rtma_record:
            rtma_record_with_dwpt = create_rtma_dwpt(rtma_record, combined_date)
            mawnsrc_record = replace_none_with_rtmarecord(
                mawnsrc_record, rtma_record_with_dwpt, combined_date
            )

        # Append the cleaned MAWN source record to the list of clean records
        clean_records.append(mawnsrc_record)

        # Extract the keys of the cleaned record for further processing (e.g., inserting into a database)
        record_keys = list(mawnsrc_record.keys())

        # Extract the values of the cleaned record for further processing
        record_vals.append(list(mawnsrc_record.values()))

        # Set the database columns to match the keys of the cleaned record
        db_columns = record_keys

    # Return all cleaned records
    return clean_records


if __name__ == "__main__":
    clean_records = main()
    for record in clean_records:
        print(f"Clean Record: {record}")

"""
Create a function to check if the column dwpt exists in an RTMA table.
If it does have a dwpt column, and the value is None(if the value is not a valid temp), then we 
calculate it from temp and relh and insert the calculated value into the record we are working with.
Some tables in RTMA don't have dwpt in them.

some logs to check: if an rtma

"""