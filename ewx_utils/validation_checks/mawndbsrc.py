#!/usr/bin/python3
import psycopg2
import sys
sys.path.append('c:/Users/mwangija/data_file/ewx_utils/ewx_utils')
import pprint
import psycopg2.extras
import datetime
from datetime import timezone
from zoneinfo import ZoneInfo
from variables_list import relh_vars, pcpn_vars, rpet_vars, temp_vars, wspd_vars, wdir_vars, leafwt_vars
from mawndb_classes.humidity import humidity
from mawndb_classes.windspeed import windspeed
from mawndb_classes.leafwetness import leafwetness
from mawndb_classes.temperature import temperature
from mawndb_classes.precipitation import precipitation
from mawndb_classes.winddirection import winddirection
from mawndb_classes.evapotranspiration import evapotranspiration
import logging
from validation_logsconfig import validations_logger

# Initialize the logger
my_validation_logger = validations_logger()
my_validation_logger.error("Remember to log errors using my_validation_logger")

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
        rh = humidity(v, 'PCT', d)
        return rh.IsInRange() and rh.IsValid()
    if k in pcpn_vars:
        pn = precipitation(v, 'hourly', 'MM', d)
        return pn.IsValid()
    if k in rpet_vars:
        rp = evapotranspiration(v, 'hourly', 'MM', d)
        return rp.IsValid()
    if k in temp_vars:
        tp = temperature(v, 'C', d)
        return tp.IsValid()
    if k in wspd_vars:
        ws = windspeed(v, 'MPS', d)
        return ws.IsValid()
    if k in wdir_vars:
        wd = winddirection(v, 'DEGREES', d)
        return wd.IsValid()
    if k in leafwt_vars:
        lw = leafwetness(v, 'hourly', k, d)
        return lw.IsValid() and lw.IsWet()
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

def creating_mawnsrc_record(record: dict, combined_datetime: datetime.datetime, id_col_list: list) -> dict:
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
            value_check = check_value(key, mawnsrc_record[key], combined_datetime)
            mawnsrc_record[key + '_src'] = "MAWN" if value_check else "EMPTY"
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
        if key in relh_vars and 100 <= mawnsrc_record[key] <= 105:
            mawnsrc_record[key + '_src'] = 'RELH_CAP'
            mawnsrc_record[key] = 100
        elif key in relh_vars and mawnsrc_record[key + '_src'] == 'EMPTY':
            mawnsrc_record[key] = None
    return mawnsrc_record

def replace_none_with_rtmarecord(mawnsrc_record: dict, rtma_record: dict, combined_datetime: datetime.datetime) -> dict:
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
        if key.endswith('_src'):
            data_key = key[:-4]
            if mawnsrc_record.get(data_key) is None and rtma_record.get(data_key) is not None:
                if check_value(data_key, rtma_record[data_key], combined_datetime):
                    clean_record[data_key] = rtma_record[data_key]
                    clean_record[key] = 'RTMA'
                else:
                    clean_record[data_key] = None
                    clean_record[key] = 'EMPTY'
    return clean_record

def one_mawndb_record(mawndb_records: list) -> list:
    """
    Converts a list of MAWN DB records into a list of dictionaries.

    Args:
        mawndb_records (list): List of MAWN DB records.

    Returns:
        list: List of dictionaries representing the MAWN DB records.
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
    """
    # Example Records
    rtma_records = [
        {
            'year': 2022, 'day': 68, 'hour': 24, 'rpt_time': 2400, 'date': datetime.date(2022, 3, 10), 'time': datetime.time(0, 0), 
            'atmp': -4.92, 'relh': 81.35, 'dwpt': None, 'pcpn': 0.0, 'lws0_pwet': None, 'lws1_pwet': None, 'wspd': 0.938, 'wdir': 291.6, 
            'wspd_max': 6.897, 'srad': None, 'stmp_05cm': None, 'stmp_10cm': None, 'stmp_20cm': None, 'stmp_50cm': None, 'smst_05cm': None, 
            'smst_10cm': None, 'smst_20cm': None, 'smst_50cm': None, 'rpet': None, 'id': 18720707
        }, 
        {
            'year': 2022, 'day': 69, 'hour': 1, 'rpt_time': 100, 'date': datetime.date(2022, 3, 10), 'time': datetime.time(1, 0), 
            'atmp': -5.17, 'relh': 79.35, 'dwpt': None, 'pcpn': 0.0, 'lws0_pwet': None, 'lws1_pwet': None, 'wspd': 0.631, 'wdir': 253.2, 
            'wspd_max': 6.051, 'srad': None, 'stmp_05cm': None, 'stmp_10cm': None, 'stmp_20cm': None, 'stmp_50cm': None, 'smst_05cm': None, 
            'smst_10cm': None, 'smst_20cm': None, 'smst_50cm': None, 'rpet': None, 'id': 18721453
        }
    ]
    mawndb_records = [
        {
            'year': 2022, 'day': 68, 'hour': 24, 'rpt_time': 2400, 'date': datetime.date(2022, 3, 10), 'time': datetime.time(0, 0), 
            'atmp': -4.9, 'relh': 81, 'dwpt': -7.58, 'pcpn': 0.0, 'lws0_pwet': 0, 'lws1_pwet': 0, 'wspd': 0.9, 'wdir': 292.0, 'wspd_max': 6.5, 
            'srad': 0.0, 'stmp_05cm': 1.2, 'stmp_10cm': 1.3, 'stmp_20cm': 1.8, 'stmp_50cm': 2.9, 'smst_05cm': 0.273, 'smst_10cm': 0.283, 
            'smst_20cm': 0.293, 'smst_50cm': 0.337, 'rpet': 0.0, 'id': 18720707
        }, 
        {
            'year': 2022, 'day': 69, 'hour': 1, 'rpt_time': 100, 'date': datetime.date(2022, 3, 10), 'time': datetime.time(1, 0), 
            'atmp': -5.0, 'relh': 79, 'dwpt': -8.4, 'pcpn': 0.0, 'lws0_pwet': 0, 'lws1_pwet': 0, 'wspd': 0.7, 'wdir': 253.0, 'wspd_max': 5.8, 
            'srad': 0.0, 'stmp_05cm': 1.2, 'stmp_10cm': 1.2, 'stmp_20cm': 1.7, 'stmp_50cm': 2.8, 'smst_05cm': 0.273, 'smst_10cm': 0.282, 
            'smst_20cm': 0.291, 'smst_50cm': 0.334, 'rpet': 0.0, 'id': 18721453
        }
    ]

    # Convert records to dictionaries
    mawndb_records = one_mawndb_record(mawndb_records)
    rtma_records = one_rtma_record(rtma_records)

    # Iterate over records and perform validation and cleaning
    for record in mawndb_records:
        combined_date = combined_datetime(record)
        id_col_list = ["year", "day", "hour", "rpt_time", "date", "time", "id"]
        mawnsrc_record = creating_mawnsrc_record(record, combined_date, id_col_list)
        mawnsrc_record = relh_cap(mawnsrc_record, relh_vars)

        rtma_record = next((r for r in rtma_records if r['id'] == record['id']), None)
        if rtma_record:
            mawnsrc_record = replace_none_with_rtmarecord(mawnsrc_record, rtma_record, combined_date)
        
        clean_records.append(mawnsrc_record)
        record_keys = list(mawnsrc_record.keys())
        record_vals.append(list(mawnsrc_record.values()))
        db_columns = record_keys
    
    # Example output
    for clean_record in clean_records:
        print(clean_record)

if __name__ == "__main__":
    main()


