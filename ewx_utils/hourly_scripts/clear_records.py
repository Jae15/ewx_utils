#!/usr/bin/env python
import os
import sys
import argparse
import datetime as datetime
from datetime import datetime, timedelta
from datetime import date
from psycopg2 import OperationalError
from pprint import pprint
from dotenv import load_dotenv
ewx_base_path = os.getenv("EWX_BASE_PATH")
sys.path.append(ewx_base_path)
from ewx_utils.ewx_config import ewx_log_file
from ewx_utils.db_files.dbs_connections import (
    connect_to_mawn_dbh11, connect_to_mawn_supercell,
    connect_to_mawnqc_dbh11, connect_to_mawnqc_supercell,
    connect_to_mawnqcl, connect_to_rtma_dbh11,
    connect_to_rtma_supercell, connect_to_mawnqc_test,
    mawn_dbh11_cursor_connection, mawn_supercell_cursor_connection,
    mawnqc_dbh11_cursor_connection, mawnqc_supercell_cursor_connection,
    mawnqcl_cursor_connection, rtma_dbh11_cursor_connection,
    rtma_supercell_cursor_connection, mawnqc_test_cursor_connection
)
from ewx_utils.validation_checks.hourly_validation_utils import process_records
from ewx_utils.logs.ewx_utils_logs_config import ewx_utils_logger
from typing import List, Dict, Any

load_dotenv()
# Initialize the logger
my_logger = ewx_utils_logger(log_path = ewx_log_file)

def create_db_connections(args):
    """
    Creating and returning only the necessary database connections and cursors for the QC database,
    based on the user-specified arguments.
    """
    my_logger.info("Creating necessary database connections for clearing records.")
    connections = {}

    try:
        # Connect to the appropriate QC database based on the args.qcwrite value
        if args.qcwrite == 'mawnqc_test:local':
            my_logger.info("Connecting to QC Test database (local).")
            connections['qctest_connection'] = connect_to_mawnqc_test()
            connections['qctest_cursor'] = mawnqc_test_cursor_connection(connections['qctest_connection'])
        elif args.qcwrite == 'mawnqcl:local':
            my_logger.info("Connecting to MAWNQCL database (local).")
            connections['mawnqcl_connection'] = connect_to_mawnqcl()
            connections['mawnqcl_cursor'] = mawnqcl_cursor_connection(connections['mawnqcl_connection'])
        elif args.qcwrite == 'mawnqc:dbh11':
            my_logger.info("Connecting to MAWNQC DBH11 database.")
            connections['mawnqc_dbh11_connection'] = connect_to_mawnqc_dbh11()
            connections['mawnqc_dbh11_cursor'] = mawnqc_dbh11_cursor_connection(connections['mawnqc_dbh11_connection'])
        elif args.qcwrite == 'mawnqc:supercell':
            my_logger.info("Connecting to MAWNQC Supercell database.")
            connections['mawnqc_supercell_connection'] = connect_to_mawnqc_supercell()
            connections['mawnqc_supercell_cursor'] = mawnqc_supercell_cursor_connection(connections['mawnqc_supercell_connection'])

        my_logger.info("Database connections created successfully for clearing records.")
        return connections

    except OperationalError as e:
        my_logger.error(f"OperationalError: {e}")
        close_connections(connections)
        raise
    except Exception as e:
        my_logger.error(f"Unexpected error: {e}")
        close_connections(connections)
        raise


def get_all_stations_list(cursor) -> List[str]:
    """
    Fetch station names from the database and log them, excluding 'variables_hourly'.
    """
    query = """SELECT table_name FROM information_schema.tables
               WHERE table_schema = 'public' AND table_name LIKE '%hourly'
               ORDER BY table_name ASC"""
    
    try:
        if cursor is None:
            my_logger.error("Cursor is not valid.")
            return []

        cursor.execute(query)
        stations = cursor.fetchall()

        if not stations:
            my_logger.warning("No stations found.")
            return []

        # Creating a list of station names and logging them
        stations_list = [row[0] for row in stations]  # Assuming row is a tuple
        my_logger.info(f"Fetched stations: {stations_list}")

        # Exclude the station named 'variables_hourly'
        excluded_stations = {'variables_hourly'}
        cleaned_stations_list = [
            station.replace('_hourly', '') for station in stations_list 
            if station not in excluded_stations
        ]
        return cleaned_stations_list

    except Exception as e:
        my_logger.error(f"An error occurred: {e}")
        return []

def clear_records(cursor, station):
    """
    Delete all records from the specified station.
    """
    try:
        delete_query = f"DELETE FROM {station}_hourly"
        cursor.execute(delete_query)
        my_logger.info(f"Deleted all records from {station}.")
    except Exception as e:
        my_logger.error(f"Error deleting records from {station}: {e}")
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

def clear_and_commit(connection, station):
    try:
        with connection.cursor() as cursor:
            clear_records(cursor, station)
            connection.commit()  # Commit after successful execution
            my_logger.info("Successfully committed transaction")
    except Exception as e:
        connection.rollback()  # Rollback in case of error
        my_logger.error(f"Transaction failed and rolled back: {e}")

def main():
    # Initialize argument parser
    parser = argparse.ArgumentParser(
        prog='clear_records',
        description='Clear records from specified stations in the QC database.'
    )
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-x', '--execute', action='store_true', help='Execute SQL and clear data in QC database')
    group.add_argument('-d', '--dryrun', action='store_true', help='Do not execute SQL, just log the actions')

    group = parser.add_mutually_exclusive_group()
    group.add_argument('-s', '--stations', nargs='*', type=str, help='Run for specific stations (list station names)')
    group.add_argument('-a', '--all', action='store_true', default=False, help='Run for all stations')

    parser.add_argument('-q', '--qcwrite', type=str, default='mawnqc_test:local',
                        choices=['mawnqc_test:local', 'mawnqcl:local', 'mawnqc:dbh11', 'mawnqc:supercell'],
                        help='Modify data in a specific database')

    args = parser.parse_args()

    # Establish database connections based on args
    db_connections = create_db_connections(args)

    try:
        # Use the necessary cursor for the QC database
        qctest_cursor = db_connections.get('qctest_cursor')

        # Determine the list of stations to clear
        if args.all:
            stations = get_all_stations_list(qctest_cursor)
        else:
            stations = args.stations

        # Clear records for specified stations
        for station in stations:
            if args.execute:
                clear_and_commit(db_connections['qctest_connection'], station)
            else:
                my_logger.info(f"Dry run: Would clear records for station {station}")

    except Exception as e:
        my_logger.error(f"An error occurred: {e}")
    finally:
        # Close all database connections
        close_connections(db_connections)

if __name__ == "__main__":
    main()

"""
usage: clear_records [-h] (-x | -d) [-s [STATIONS ...] | -a] [-q {mawnqc_test:local,mawnqcl:local,mawnqc:dbh11,mawnqc:supercell}]
clear_records: error: one of the arguments -x/--execute -d/--dryrun is required
# To clear records from all the stations
python clear_records.py -x -a -q mawnqc_test:local
# To clear records from specific stations
python clear_records.py -x -s station1 station2 -q mawnqc_test:local
# For a dry-run
python clear_records.py -d -a -q mawnqc_test:local

"""
