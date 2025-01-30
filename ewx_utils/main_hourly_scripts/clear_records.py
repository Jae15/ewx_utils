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
from ewx_utils.validation_checks.main_validation_utils import process_records
from ewx_utils.logs.ewx_utils_logs_config import ewx_utils_logger
from typing import List, Dict, Any

load_dotenv()
# Initialize the logger
my_logger = ewx_utils_logger(log_path=ewx_log_file)


def create_db_connections(args):
    """
    Creating and returning only the necessary database connections and cursors for the QC database,
    based on the user-specified arguments.
    """
    my_logger.info("Creating necessary database connections for clearing records.")
    connections = {}

    try:
        my_logger.info("args.qcwrite")
        print(f"args.qcwrite: {args.qcwrite}")
        # Connect to the appropriate QC database based on the args.qcwrite value
        if args.qcwrite.strip() == "mawnqc_test:local":
            my_logger.info("Connecting to QC Test database (local).")
            connections["qcwrite_connection"] = connect_to_mawnqc_test()
            connections["qcwrite_cursor"] = mawnqc_test_cursor_connection(
                connections["qcwrite_connection"]
            )
        elif args.qcwrite == "mawnqcl:local":
            my_logger.info("Connecting to MAWNQCL database (local).")
            connections["qcwrite_connection"] = connect_to_mawnqcl()
            connections["qcwrite_cursor"] = mawnqcl_cursor_connection(
                connections["mawnqcl_connection"]
            )
        elif args.qcwrite == "mawnqc:dbh11":
            my_logger.info("Connecting to MAWNQC DBH11 database.")
            connections["qcwrite_connection"] = connect_to_mawnqc_dbh11()
            connections["qcwrite_cursor"] = mawnqc_dbh11_cursor_connection(
                connections["mawnqc_dbh11_connection"]
            )
        elif args.qcwrite == "mawnqc:supercell":
            my_logger.info("Connecting to MAWNQC Supercell database.")
            connections["qcwrite_connection"] = connect_to_mawnqc_supercell()
            connections["qcwrite_cursor"] = mawnqc_supercell_cursor_connection(
                connections["mawnqc_supercell_connection"]
            )
        else:
            raise ValueError(f" No match for argument qcwrite")

        my_logger.info(
            "Database connections created successfully for clearing records."
        )
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
    Retrieve station names from the database, excluding 'variables_hourly'.

    Args:
        cursor: Database cursor for executing queries.

    Returns:
        List of station names without '_hourly' suffix, or an empty list if an error occurs.
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
        excluded_stations = {"variables_hourly"}
        cleaned_stations_list = [
            station.replace("_hourly", "")
            for station in stations_list
            if station not in excluded_stations
        ]
        return cleaned_stations_list

    except Exception as e:
        my_logger.error(f"An error occurred: {e}")
        return []


def clear_records(cursor, station):
    """
    Delete all records from the specified station's hourly table.

    Parameters:
        cursor: A database cursor for executing queries.
        station: The name of the station from which to delete records.

    Raises:
        Exception: If an error occurs during the deletion process.
    """
    try:
        delete_query = f"DELETE FROM {station}_hourly"
        print(f"Executing query: {delete_query}")
        cursor.execute(delete_query)
        my_logger.info(f"Deleted all records from {station}.")
    except Exception as e:
        print(f"Error during deletion: {e}")
        my_logger.error(f"Error deleting records from {station}: {e}")
        raise


def close_connections(connections):
    """
    Close all database connections and cursors.

    Parameters:
        connections (dict): A dictionary of connection and cursor objects,
                            where keys are names and values are the respective objects.

    Raises:
        Exception: If an error occurs while closing any connection or cursor.
    """
    my_logger.info("Closing database connections.")
    for name, conn in connections.items():
        try:
            if "cursor" in name:
                conn.close()
                my_logger.info(f"{name} cursor closed.")
            elif "connection" in name:
                conn.close()
                my_logger.info(f"{name} connection closed.")
        except Exception as e:
            my_logger.error(f"Error closing {name}: {e}")


def clear_and_commit(connection, station):
    """
    Clear records from the specified station and commit the transaction.

    Parameters:
        connection: A database connection object used to execute commands.
        station: The name of the station from which to clear records.

    Raises:
        Exception: If an error occurs during the clearing or committing process.
    """
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
        prog="clear_records",
        description="Clear records from specified stations in the QC database.",
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "-x",
        "--execute",
        action="store_true",
        help="Execute SQL and clear data in QC database",
    )
    group.add_argument(
        "-d",
        "--dryrun",
        action="store_true",
        help="Do not execute SQL, just log the actions",
    )

    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "-s",
        "--stations",
        nargs="*",
        type=str,
        help="Run for specific stations (list station names)",
    )
    group.add_argument(
        "-a", "--all", action="store_true", default=False, help="Run for all stations"
    )

    parser.add_argument(
        "-q",
        "--qcwrite",
        type=str,
        default="mawnqc_test:local",
        choices=["mawnqc_test:local", "mawnqcl:local"],
        help="Modify data in a specific database",
    )

    args = parser.parse_args()
    print(f"Parsed Arguments: {args}")

    # Establish database connections based on args
    db_connections = create_db_connections(args)
    print(f"Database Connections: {db_connections}")

    try:
        # Use the necessary cursor for the QC database
        qcwrite_cursor = db_connections.get("qcwrite_cursor")
        print(f"Cursor Obtained: {qcwrite_cursor}")

        # Determine the list of stations to clear
        if args.all:
            stations = get_all_stations_list(qcwrite_cursor)
        else:
            stations = args.stations
            print(f"Stations to clear: {stations}")

        # Clear records for specified stations
        for station in stations:
            print(f"Processing station: {station}")
            if args.execute and qcwrite_cursor:
                clear_and_commit(db_connections["qcwrite_connection"], station)
            else:
                my_logger.info(f"Dry run: Would clear records for station {station}")
                print(f"Dry run for station: {station}")

    except Exception as e:
        print(f"Exception occurred: {e}")
        my_logger.error(f"An error occurred: {e}")
    finally:
        # Close all database connections
        close_connections(db_connections)


if __name__ == "__main__":
    main()

"""
python clear_records.py -x -s arlene -q mawnqc_test:local
usage: clear_records [-h] (-x | -d) [-s [STATIONS ...] | -a] [-q {mawnqc_test:local,mawnqcl:local,mawnqc:dbh11,mawnqc:supercell}]
clear_records: error: one of the arguments -x/--execute -d/--dryrun is required
# To clear records from all the stations
python clear_records.py -x -a -q mawnqc_test:local
# To clear records from specific stations
python clear_records.py -x -s station1 station2 -q mawnqc_test:local
# For a dry-run
python clear_records.py -d -a -q mawnqc_test:local
"""
