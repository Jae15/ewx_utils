"""
Pseudocode: 

Objective 2:
Write a utility script - given a date range, station, and 2 databases: 
a) does every record (identified by date and time) in test database exist in supercell database? 
b) does every record (identified by date and time) in supercell database exist in test database?
c) does every record in test database match exactly the record in supercell?
 
To get to objective 2: 
 
query all records in test database - get the combined datetime - compare to timeloop. 
 
query all records in supercell database - get combined datetime for each - compare to timeloop.
 
if they both match the timeloop - then you have all the records.  If not - we need to fix something.
 
write a function that compares one record to another record - are they the same?  if so, output something.  If not output something else?

"""

import argparse
import pprint
import logging
from datetime import datetime
from ewxutils_logsconfig import ewx_utils_logger
from db_files.dbconnection import (
    connect_to_mawnqc_test,
    connect_to_mawnqc_supercell,
    mawnqc_test_cursor_connection,
    mawn_supercell_cursor_connection,
)

# Initialize the logger
my_logger = ewx_utils_logger()


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
    Comparing the two sets of records.
    """
    test_records_dict = {(rec['date'], rec['time']): rec for rec in test_records}
    supercell_records_dict = {(rec['date'], rec['time']): rec for rec in supercell_records}

    only_in_test = []
    only_in_supercell = []
    mismatches = []
    
    for key in test_records_dict:
        if key not in supercell_records_dict:
            only_in_test.append(test_records_dict[key])
        elif test_records_dict[key] != supercell_records_dict[key]:
            details = []
            for column_name in test_records_dict[key].keys():
                if column_name in supercell_records_dict[key].keys() and key != "id":
                    if test_records_dict[key][column_name] != supercell_records_dict[key][column_name]:
                        details.append(column_name) 
                elif column_name not in supercell_records_dict[key].keys():
                    details.append(column_name)

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
    #parser.add_argument('--test_database', type=str, default='mawnqc_test', help="Test table name")
    #parser.add_argument('--supercell_database', type=str, default='mawndb_qc', help="Supercell table name")

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
            pprint.pprint(only_in_test)
        if only_in_supercell:
            my_logger.error(f"Records found only in supercell database: {len(only_in_supercell)}")
            pprint.pprint(only_in_supercell)
        if mismatches:
            my_logger.error(f"Mismatched records: {len(mismatches)}")
            for mismatch in mismatches:
                my_logger.error("Test Record:")
                pprint.pprint(mismatch[0])
                my_logger.error("Supercell Record:")
                pprint.pprint(mismatch[1])
                my_logger.error("Details: ")
                pprint.pprint(mismatch[2])


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

