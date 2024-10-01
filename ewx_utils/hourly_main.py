import psycopg2
import os
import sys
import time
import shutil
import getopt
import datetime as datetime
import pprint
import argparse
import logging
from datetime import timezone
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta
from datetime import date, time
from psycopg2 import OperationalError
from db_files.dbconnection import (
    connect_to_mawn_dbh11,
    connect_to_mawn_supercell,
    connect_to_mawnqc_dbh11,
    connect_to_mawnqc_supercell,
    connect_to_mawnqcl,
    connect_to_rtma_dbh11,
    connect_to_rtma_supercell,
    connect_to_mawnqc_test,
    mawn_dbh11_cursor_connection,
    mawn_supercell_cursor_connection,
    mawnqc_dbh11_cursor_connection,
    mawnqc_supercell_cursor_connection,
    mawnqcl_cursor_connection,
    rtma_dbh11_cursor_connection,
    rtma_supercell_cursor_connection,
    mawnqc_test_cursor_connection,
)
from validation_checks.timeloop import generate_list_of_hours
from validation_checks.mawndbsrc import process_records
from ewxutils_logsconfig import ewx_utils_logger

# Initialize the logger
my_logger = ewx_utils_logger()

def create_db_connections():
    """
    Create and return database connections and cursors for mawndb, mawndbqc, rtma, and qctest databases.
    """
    my_logger.error("Creating database connections.")
    connections = {}
    try:
        # Connect to various databases and get cursor objects
        connections['mawn_dbh11_connection'] = connect_to_mawn_dbh11()
        connections['mawn_dbh11_cursor'] = mawn_dbh11_cursor_connection(connections['mawn_dbh11_connection'])

        connections['mawn_supercell_connection'] = connect_to_mawn_supercell()
        connections['mawn_supercell_cursor'] = mawn_supercell_cursor_connection(connections['mawn_supercell_connection'])

        connections['mawnqc_dbh11_connection'] = connect_to_mawnqc_dbh11()
        connections['mawnqc_dbh11_cursor'] = mawnqc_dbh11_cursor_connection(connections['mawnqc_dbh11_connection'])

        connections['mawnqc_supercell_connection'] = connect_to_mawnqc_supercell()
        connections['mawnqc_supercell_cursor'] = mawnqc_supercell_cursor_connection(connections['mawnqc_supercell_connection'])

        connections['rtma_dbh11_connection'] = connect_to_rtma_dbh11()
        connections['rtma_dbh11_cursor'] = rtma_dbh11_cursor_connection(connections['rtma_dbh11_connection'])

        connections['rtma_supercell_connection'] = connect_to_rtma_supercell()
        connections['rtma_supercell_cursor'] = rtma_supercell_cursor_connection(connections['rtma_supercell_connection'])

        connections['qctest_connection'] = connect_to_mawnqc_test()
        connections['qctest_cursor'] = mawnqc_test_cursor_connection(connections['qctest_connection'])

        my_logger.error("Database connections created successfully.")
        return connections

    except OperationalError as e:
        my_logger.error(f"OperationalError: {e}")
        close_connections(connections)
        raise
    except Exception as e:
        my_logger.error(f"Unexpected error: {e}")
        close_connections(connections)
        raise

def close_connections(connections):
    """
    Close all database connections and cursors.
    """
    my_logger.info("Closing database connections.")
    for name, conn in connections.items():
        try:
            if 'cursor' in name:
                conn.close()
                my_logger.info(f"{name} cursor closed.")
            elif 'connection' in name:
                conn.close()
                my_logger.info(f"{name} connection closed.")
        except Exception as e:
            my_logger.error(f"Error closing {name}: {e}")

def commit_and_close(connection):
    """
    Commit the transaction and close the database connection.
    """
    try:
        connection.commit()
        connection.close()
        my_logger.info("Transaction committed and connection closed.")
    except Exception as e:
        my_logger.error(f"Error committing transaction or closing connection: {e}")
        raise

def fetch_records(cursor, station, begin_date, end_date):
    """
    Fetch records from the specified table and date.
    """
    query = f"SELECT * FROM {station} WHERE date BETWEEN '{begin_date}' AND '{end_date}'"
    my_logger.info(f"Executing query: {query}")
    try:
        cursor.execute(query)
        records = cursor.fetchall()
        my_logger.info(f"Fetched {len(records)} records from {station} for date range {begin_date} to {end_date}.")
        return [dict(record) for record in records]
    except Exception as e:
        my_logger.error(f"Error fetching records from {station}: {e}")
        raise

def insert_records(cursor, station, records):
    """
    Insert records into the specified table.
    """
    if not records:
        my_logger.warning(f"No records to insert into {station}.")
        return

    try:
        record_keys = list(records[0].keys())
    except (IndexError, AttributeError, KeyError) as e:
        my_logger.error(f"Error accessing record keys: {e}")
        return

    db_columns = ", ".join(record_keys)
    qc_values = ", ".join(["%s"] * len(record_keys))
    query = f"INSERT INTO {station} ({db_columns}) VALUES ({qc_values})"
    my_logger.info(f"Constructed INSERT query: {query}")

    try:
        for record in records:
            record_vals = list(record.values())
            cursor.execute(query, record_vals)
        my_logger.info(f"Inserted {len(records)} records into {station}.")
    except Exception as e:
        my_logger.error(f"Error inserting records into {station}: {e}")
        raise

def time_defaults(user_begin_date: str, user_end_date: str):
    """
    Set default begin and end dates if not provided, and ensure the dates are returned as strings.
    """
    try:
        # Default begin_date is 7 days ago if not provided
        if user_begin_date is None:
            user_begin_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        else:
            user_begin_date = datetime.strptime(user_begin_date, '%Y-%m-%d').strftime('%Y-%m-%d')

        # Default end_date is 7 days from begin_date if not provided
        if user_end_date is None:
            user_end_date = (datetime.strptime(user_begin_date, '%Y-%m-%d') + timedelta(days=7)).strftime('%Y-%m-%d')
        else:
            user_end_date = datetime.strptime(user_end_date, '%Y-%m-%d').strftime('%Y-%m-%d')

        # Validate that the begin_date comes before the end_date
        if user_begin_date > user_end_date:
            raise ValueError("Begin date should come before end date.")

        return user_begin_date, user_end_date

    except ValueError as ve:
        my_logger.error(f"ValueError occurred: {ve}")
        print("ValueError:", ve)
    except Exception as e:
        my_logger.error(f"An error occurred: {e}")
        print("An error occurred:", e)
    finally:
        my_logger.info("time_defaults function execution completed.")

def main():
    parser = argparse.ArgumentParser(
        prog='hourly_main',
        description='Checks data from hourly_main in mawndb_qc and adds estimates from RTMA or fivemin data as needed',
        epilog='Check missing data and ask python scripts for help'
    )
    parser.add_argument('-b', '--begin', type=str, help='Start date (no time accepted)')
    parser.add_argument('-e', '--end', type=str, help='End date (no time accepted)')
    parser.add_argument('-f', '--forcedelete', action='store_true', help="Force delete old records")
    parser.add_argument('-c', '--clearoverride', action='store_true', help="Clear override active flag")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-x', '--execute', action='store_true', help='Execute SQL and change data in QC database')
    group.add_argument('-d', '--dryrun', action='store_true', help='Do not execute SQL, just write to stdout/store data in test database')
    parser.add_argument('-l', '--log', action='store_true', help='Create a logfile for this run')

    group = parser.add_mutually_exclusive_group()
    group.add_argument('-s', '--stations', nargs='*', type=str, help='Run for specific stations (list station names)')
    group.add_argument('-a', '--all', action='store_true', default=False, help='Run for all stations')

    parser.add_argument('-q', '--qcwrite', type=str, default='mawnqc_test:local',
                        choices=['mawnqc_test:local', 'mawnqcl:local', 'mawnqc:dbh11', 'mawnqc:supercell'],
                        help='Modify data in a specific database')
    parser.add_argument("--mawn", type=str, choices=['mawn:dbh11'], default='mawn:dbh11',
                        help='Read mawndb data from a specific database')
    parser.add_argument("--rtma", type=str, choices=['rtma:dbh11'], default='rtma:dbh11',
                        help='Read rtma data from a specific database')

    args = parser.parse_args()

    # Parse and set date ranges
    begin_date, end_date = time_defaults(args.begin, args.end)

    # Establish database connections
    db_connections = create_db_connections()

    try:
        if args.mawn == 'mawn:dbh11':
            mawn_cursor = db_connections["mawn_dbh11_cursor"]
        if args.rtma == 'rtma:dbh11':
            rtma_cursor = db_connections["rtma_dbh11_cursor"]
        if args.qcwrite == 'mawnqc_test:local':
            qctest_cursor = db_connections["qctest_cursor"]

        for station in args.stations:
            mawn_records = fetch_records(mawn_cursor, station, begin_date, end_date)
            rtma_records = fetch_records(rtma_cursor, station, begin_date, end_date)

            cleaned_records = process_records(mawn_records, rtma_records, begin_date, end_date)

            if args.execute:
                insert_records(qctest_cursor, station, cleaned_records)

    finally:
        # Commit and close the connections
        commit_and_close(db_connections["mawn_dbh11_connection"])
        commit_and_close(db_connections["rtma_dbh11_connection"])
        commit_and_close(db_connections["mawnqc_dbh11_connection"])
        commit_and_close(db_connections["qctest_connection"])
        close_connections(db_connections)

if __name__ == "__main__":
    main()

"""
python hourly_main.py --begin 2023-1-1 --end 2023-1-7 --station aetna_hourly -x

hourly_main [-h] [-b BEGIN] [-e END] [-f] [-c] (-x | -d) [-l] [-s [STATIONS ...] | -a]
[-q {mawnqc_test:local,mawnqcl:local,mawnqc:dbh11,mawnqc:supercell}] [--mawn {mawn:dbh11}] [--rtma {rtma:dbh11}]


"""