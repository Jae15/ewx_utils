#!/usr/bin/env python
import os
import sys
import decimal
import argparse
import pprint
from pprint import pprint
import dotenv
dotenv.load_dotenv()
from dotenv import load_dotenv
load_dotenv()
ewx_base_path = os.getenv("EWX_BASE_PATH")
sys.path.append(ewx_base_path)
from datetime import datetime
from ewx_utils.ewx_config import ewx_log_file
from ewx_utils.logs.ewx_utils_logs_config import ewx_utils_logger

# Initialize the logger
my_logger = ewx_utils_logger(log_path = ewx_log_file)
from ewx_utils.db_files.dbs_connections import (
    connect_to_mawnqc_test,
    connect_to_mawnqc_supercell,
    mawnqc_test_cursor_connection,
    mawn_supercell_cursor_connection,
)
# Initialize the logger
my_logger = ewx_utils_logger()

def limit_to_max_digits(num, max_digits=None):
    """
    Rounds a number to the specified maximum of significant digits.
    It follows PostgreSQL-like rounding rules i.e (ROUND_HALF_UP)

    Args:
    num(float): The number to round.
    max_digits(int): The maximum number of significant digits allowed(default is 6)

    Returns:
    float: The number rounded according to PostgreSQL-like behavior
    """
    if max_digits is None:
        max_digits = 6

    if num == 0:
        return decimal.Decimal('0.0')
    
    # Create a Decimal object from the number
    num_decimal = decimal.Decimal(str(num))

    # Calculate the number of digits before the decimal point
    integer_part = num_decimal.to_integral_value()
    integer_digits = len(str(integer_part))

    # Calculate the number of decimal places to preserve
    decimal_places = max_digits - integer_digits

    # If decimal_places is negative, it means we have more than max_digits in the integer part
    if decimal_places < 0:
        decimal_places = 0
    # Create a quantizer string that matches the precision required 
    quantize_str = decimal.Decimal('1.' + '0' * decimal_places)

    # Round the number using ROUND_HALF_UP to behave like PostgreSQL
    rounded_decimal = num_decimal.quantize(quantize_str, rounding=decimal.ROUND_HALF_UP)
    return rounded_decimal

def fetch_records_by_date(cursor, station, start_date, end_date):

    """
    Fetching records from the specified table based on date range.
    """
    query = f"""
    SELECT * FROM {station}
    WHERE date BETWEEN '{start_date}' AND '{end_date}'
    """
    my_logger.error(f"Executing query: {query}")
    try:
        cursor.execute(query)
        records = cursor.fetchall()
        my_logger.error(f"Fetched {len(records)} records from {station} for date range {start_date} to {end_date}.")
        return [dict(record) for record in records]
    except Exception as e:
        my_logger.error(f"Error fetching records from {station}: {e}")
        raise


def is_within_margin(value1, value2):
    # Convert value2 to string to check decimal places
    str_value2 = str(value2)
    if '.' in str_value2:
        decimal_places = len(str_value2.split('.')[1])
    else:
        decimal_places = 0

    # Set margin based on decimal places
    if decimal_places == 0:
        margin = 0
    elif decimal_places == 1:
        margin = 0.1
    elif decimal_places == 2:
        margin = 0.01
    elif decimal_places == 3:
        margin = 0.001
    else:
        margin = 0.001  # Default for more than 3 decimal places

    return abs(value1 - value2) <= margin

def compare_records(test_records, supercell_records):
    """
    Comparing the two sets of records while ignoring the 'id' column.
    Allows a tolerance of 0.005 for 'srad' column comparisons and uses
    limit_to_max_digits only for 'srad' values.
    """
    test_records_dict = {(rec['date'], rec['time']): rec for rec in test_records}
    supercell_records_dict = {(rec['date'], rec['time']): rec for rec in supercell_records}

    only_in_test = []
    only_in_supercell = []
    mismatches = []

    for key in test_records_dict:
        if key not in supercell_records_dict:
            only_in_test.append(test_records_dict[key])
        else:
            test_record = {k: v for k, v in test_records_dict[key].items() if k != 'id'}
            supercell_record = {k: v for k, v in supercell_records_dict[key].items() if k != 'id'}
            if test_record != supercell_record:
                details = []
                for column_name in test_record.keys():
                    if column_name in supercell_record:
                        if column_name == 'srad':
                            # Apply limit_to_max_digits for 'srad' and check within margin
                            test_value = limit_to_max_digits(test_record[column_name])
                            supercell_value = limit_to_max_digits(supercell_record[column_name])
                            if not is_within_margin(test_value, supercell_value):
                                details.append(column_name)
                        else:
                            # Direct comparison for other columns without limit_to_max_digits
                            if test_record[column_name] != supercell_record[column_name]:
                                details.append(column_name)
                if details:
                    mismatches.append((test_records_dict[key], supercell_records_dict[key], details))
    for key in supercell_records_dict:
        if key not in test_records_dict:
            only_in_supercell.append(supercell_records_dict[key])
    return only_in_test, only_in_supercell, mismatches


def main():
    parser = argparse.ArgumentParser(
        description="Utility script to compare records between test and supercell databases"
    )
    parser.add_argument('-b', '--begin', type=str, required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument('-e', '--end', type=str, required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument('-s', '--station', type=str, required=True, help="Station name (which is also the table name)")
    
    args = parser.parse_args()

    try:
        # Create database connections and cursors
        test_conn = connect_to_mawnqc_test()
        test_cursor = mawnqc_test_cursor_connection(test_conn)

        supercell_conn = connect_to_mawnqc_supercell()
        supercell_cursor = mawn_supercell_cursor_connection(supercell_conn)

        # Fetch records from both databases
        test_records = fetch_records_by_date(test_cursor, args.station, args.begin, args.end)
        supercell_records = fetch_records_by_date(supercell_cursor, args.station, args.begin, args.end)

        # Compare records
        only_in_test, only_in_supercell, mismatches = compare_records(test_records, supercell_records)

        # Report results
        if only_in_test:
            my_logger.error(f"Records found only in test database: {len(only_in_test)}")
            pprint.pprint(f"Records found only in test database: {len(only_in_test)}")
        if only_in_supercell:
            my_logger.error(f"Records found only in supercell database: {len(only_in_supercell)}")
            pprint.pprint(f"Records found only in supercell database: {len(only_in_supercell)}")
        if mismatches:
            my_logger.error(f"Mismatched records: {len(mismatches)}")
            for mismatch in mismatches:
                my_logger.error(f"Test Record : {mismatch[0]}")
                pprint.pprint(f"Test Record Pre Truncation/Rounding: {mismatch[0]}")
                my_logger.error(f"Supercell Record: {mismatch[1]}")
                pprint.pprint(f"Supercell Record: {mismatch[1]}")
                my_logger.error(f"Details: {mismatch[2]}")
                pprint.pprint(f"Details: {mismatch[2]}")

    except Exception as e:
        my_logger.error(f"An error occurred: {e}")
    finally:
        # Close connections
        test_conn.close()
        supercell_conn.close()

if __name__ == "__main__":
    main()


"""
python hourly_utility.py --begin 2023-01-01 --end 2023-01-02 --station aetna_hourly  

"""

