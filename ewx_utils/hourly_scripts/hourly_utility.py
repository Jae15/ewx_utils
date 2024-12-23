#!/usr/bin/env python
import os
import sys
import decimal
import argparse
import pprint
#from pprint import pprint
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

import decimal

my_logger = ewx_utils_logger()

def limit_to_max_digits(num, max_digits=None):
    try:
        if num is None:
            my_logger.error("Input num is None.")
            return None
        
        if max_digits is None:
            max_digits = 6

        if num == 0:
            return decimal.Decimal('0.0')
        
        num_decimal = decimal.Decimal(str(num))
        integer_part = num_decimal.to_integral_value()
        integer_digits = len(str(integer_part))
        decimal_places = max_digits - integer_digits

        if decimal_places < 0:
            decimal_places = 0
        
        quantize_str = decimal.Decimal('1.' + '0' * decimal_places)
        rounded_decimal = num_decimal.quantize(quantize_str, rounding=decimal.ROUND_HALF_UP)
        return rounded_decimal

    except decimal.ConversionSyntax as e:
        my_logger.error(f"ConversionSyntax error for input num: {num}, max_digits: {max_digits}. Error: {e}")
        return None
    except Exception as e:
        my_logger.error(f"An unexpected error occurred for input num: {num}, max_digits: {max_digits}. Error: {e}")
        return None
    
def is_within_margin(value1, value2):
    try:
        if value1 is None or value2 is None:
            my_logger.error(f"One of the values is None: value1: {value1}, value2: {value2}.")
            return False
        
        # converting both values to Decimal
        try:
            value1_decimal = decimal.Decimal(value1)
            value2_decimal = decimal.Decimal(value2)
        except (ValueError, TypeError) as e:
            my_logger.error(f"Non-numeric values provided: value1: {value1}, value2: {value2}. Error: {e}")
            return False

        # Calculating the margin as 0.005% of value2
        margin = abs(value2_decimal) * decimal.Decimal('0.00005')  # 0.005% of value2

        my_logger.info(f"Comparing value1: {value1_decimal} with value2: {value2_decimal}. Calculated margin: {margin}")
        
        result = abs(value1_decimal - value2_decimal) <= margin
        my_logger.info(f"Result of comparison: {result}")

        return result

    except Exception as e:
        my_logger.error(f"An error occurred while comparing values {value1} and {value2}. Error: {e}")
        return False


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

def compare_records(test_records, supercell_records):
    """
    Compare two sets of records, ignoring the 'id' column.
    
    Conditions:
    - Tolerance of 0.005 for 'srad' comparisons using limit_to_max_digits function.
    - 'relh' between 100 and 105 is considered a match to 100 in the qc database.
    - Skip '_src' columns for years before 2017.
    
    Parameters:
    test_records (list of dict): Test records with 'date', 'time', and other variables.
    supercell_records (list of dict): Supercell records with 'date', 'time', and other variables.
    
    Returns:
    tuple: (only_in_test, only_in_supercell, mismatches_details)
    """
    test_records_dict = {(rec['date'], rec['time']): rec for rec in test_records}
    supercell_records_dict = {(rec['date'], rec['time']): rec for rec in supercell_records}
    only_in_test = []
    only_in_supercell = []
    mismatches_details = []
    
    for key in test_records_dict:
        if key not in supercell_records_dict:
            only_in_test.append(test_records_dict[key])
        else:
            # Excluding 'id' from the records for comparison
            test_record = {k: v for k, v in test_records_dict[key].items() if k != 'id'}
            supercell_record = {k: v for k, v in supercell_records_dict[key].items() if k != 'id'}
            # Extracting the year using the get method
            year = test_record.get('year')
            mismatches_details = []
            
            for column_name in test_record.keys():
                #print(column_name)
                if column_name in supercell_record:
                    # For years before 2017, we skip comparison for '_src' and 'volt' columns
                    if year < 2017:
                        if column_name.endswith('_src') or column_name == 'volt':
                            continue
                    
                    # Defining conditions for the srad values
                    if column_name == 'srad':
                        test_value = limit_to_max_digits(test_record[column_name])
                        #print(f"test_value: {test_value}")
                        supercell_value = limit_to_max_digits(supercell_record[column_name])
                        #print(f"supercell_value: {supercell_value}")
                        if not is_within_margin(test_value, supercell_value):
                            mismatches_details.append([test_record, supercell_record, column_name])
                    # Defining conditions for the relh values
                    if column_name == 'relh' and test_record.get('relh_src' == "RELH_CAP"):
                        if not (100 < test_record[column_name] <= 105):
                            if test_record[column_name] != supercell_record[column_name]:
                                mismatches_details.append([test_record, supercell_record, column_name])
                    else:
                        # Comparing values for all other columns
                        if test_record[column_name] != supercell_record[column_name]:
                            mismatches_details.append([test_record, supercell_record, column_name])
    
    for key in supercell_records_dict:
        if key not in test_records_dict:
            only_in_supercell.append(supercell_records_dict[key])
    
    return only_in_test, only_in_supercell, mismatches_details


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
        only_in_test, only_in_supercell, mismatches_details = compare_records(test_records, supercell_records)
        #my_logger.error(type(only_in_test))
        #my_logger.error(type(only_in_supercell))
        #my_logger.error(type(mismatches_details))

        # Report results
        if only_in_test:
            my_logger.error(f"Records found only in test database: {len(only_in_test)}")
            print(f"Records found only in test database: {len(only_in_test)}")
        if only_in_supercell:
            my_logger.error(f"Records found only in supercell database: {len(only_in_supercell)}")
            print(f"Records found only in supercell database: {len(only_in_supercell)}")
        if mismatches_details:
            my_logger.error(f"Mismatched records: {len(mismatches_details)}")
            #my_logger.error(type(mismatches_details))
            my_logger.error(mismatches_details)
            for mismatch in mismatches_details:
                my_logger.error(f"Test Record : {mismatch[0]}")
                print(f"Test Record Pre Truncation/Rounding: {mismatch[0]}")
                my_logger.error(f"Supercell Record: {mismatch[1]}")
                print(f"Supercell Record: {mismatch[1]}")
                my_logger.error(f"Details: {mismatch[2]}")
                print(f"Details: {mismatch[2]}")

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

