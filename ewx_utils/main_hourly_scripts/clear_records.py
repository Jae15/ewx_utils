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
load_dotenv()
ewx_base_path = os.getenv("EWX_BASE_PATH")
sys.path.append(ewx_base_path)
from ewx_utils.ewx_config import ewx_log_file
from ewx_utils.db_files.dbs_configfile import get_db_config, get_ini_section_info
from ewx_utils.db_files.dbs_connection import(
    connect_to_db,
    get_mawn_cursor,
    get_rtma_cursor,
    get_qcwrite_cursor,
    create_db_connections
)
from ewx_utils.hourly_validation_checks.hourly_validation_utils import process_records
from ewx_utils.logs.ewx_utils_logs_config import ewx_utils_logger
from typing import List, Dict, Any

load_dotenv()
# Initialize the logger
my_logger = ewx_utils_logger(log_path=ewx_log_file)


def get_all_stations_list(cursor: Any) -> List[str]:
    """
    Fetch and return station names from the database, excluding 'variables_hourly'.

    Parameters:
    cursor (object): Database cursor for executing queries.

    Returns:
    list[str]: List of station names without the '_hourly' suffix.
    """
    query = """SELECT table_name FROM information_schema.tables
               WHERE table_schema = 'public' AND table_name LIKE '%hourly'
               ORDER BY table_name ASC"""

    try:
        cursor.execute(query)
        stations = cursor.fetchall()

        if not stations:
            my_logger.warning("No stations found.")
            return []

        # Creating a list of station names and logging them
        stations_list = [dict(row)["table_name"] for row in stations]
        my_logger.error(f"Fetched stations: {stations_list}")

        # Exclude the station named 'variables_hourly'
        filtered_stations_list = [
            station for station in stations_list if station != "variables_hourly"
        ]

        # Remove the '_hourly' suffix from each station name
        cleaned_stations_list = [
            station.replace("_hourly", "") for station in filtered_stations_list
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
        my_logger.error(f"Executing query: {delete_query}")
        cursor.execute(delete_query)
        my_logger.error(f"Deleted all records from {station}.")
    except Exception as e:
        #print(f"Error during deletion: {e}")
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
    my_logger.error("Closing database connections.")
    for name, conn in connections.items():
        try:
            if "cursor" in name:
                conn.close()
                my_logger.error(f"{name} cursor closed.")
            elif "connection" in name:
                conn.close()
                my_logger.error(f"{name} connection closed.")
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
            my_logger.error("Successfully committed transaction")
    except Exception as e:
        connection.rollback()  # Rollback in case of error
        my_logger.error(f"Transaction failed and rolled back: {e}")

def confirm_deletion(station_count, stations):
    """
    Ask user for confirmation before deleting records.
    
    Parameters:
    station_count: int or str - Number of stations or "all"
    stations: list - List of station names
    
    Returns:
    bool - True if user confirms, False otherwise
    """
    if station_count == "all":
        prompt = "Are you sure you want to proceed with deleting records from all stations? (yes/no): "
    elif station_count == 1:
        prompt = f"Are you sure you want to delete records from station '{stations[0]}'? (yes/no): "
    else:
        prompt = f"Are you sure you want to proceed with deleting records from {station_count} stations ({', '.join(stations)})? (yes/no): "
    
    confirmation = input(prompt)
    return confirmation.lower() in ["yes", "y"]


def main() -> None:
    """
    Main function to clear records from stations in the QC database.

    Command-line arguments:
    -s, --stations: Clear specific stations (list station names)
    -a, --all: Clear all stations
    --write-to: Section name in INI file for the QC database
    """
    ini_file_path = "path_to_ini_file.ini"
    section_info_help = get_ini_section_info(ini_file_path)
    
    # Initialize argument parser
    parser = argparse.ArgumentParser(
        prog="clear_records",
        description="Clears data from stations in the QC database",
        epilog="Use with caution - this will delete all records from specified stations",
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "-s",
        "--stations",
        nargs="*",
        type=str,
        help="Clear specific stations (list station names)",
    )
    group.add_argument(
        "-a", 
        "--all", 
        action="store_true", 
        default=False, 
        help="Clear all stations"
    )

    parser.add_argument(
        "--write-to",
        type=str,
        required=True,
        help="Section name in INI file for the QC database",
    )

    args = parser.parse_args()

    # Create database connection
    db_connections = create_db_connections(args)
    
    try:
        # Get QC database cursor
        qcwrite_cursor = get_qcwrite_cursor(db_connections['qcwrite_connection'], 'qcwrite')
        my_logger.info("Successfully connected to QC database")

        # Get list of stations to clear
        if args.all:
            stations = get_all_stations_list(qcwrite_cursor)
            station_count = "all"
            my_logger.info("Preparing to clear all stations")
        else:
            stations = args.stations
            station_count = len(stations)
            my_logger.info(f"Preparing to clear specified stations: {', '.join(stations)}")

        # User confirmation
        if not confirm_deletion(station_count, stations):
            my_logger.info("Deletion canceled by user")
            return

        # Clear records for each station
        for station in stations:
            try:
                clear_records(qcwrite_cursor, station)
                db_connections['qcwrite_connection'].commit()
                my_logger.info(f"Successfully cleared records from {station}")
            except Exception as e:
                my_logger.error(f"Failed to clear {station}: {e}")
                db_connections['qcwrite_connection'].rollback()

    except Exception as e:
        my_logger.error(f"An error occurred: {e}")
        raise

    finally:
        # Close all database connections
        close_connections(db_connections)
        my_logger.info("Database connections closed")
        my_logger.info("Deletion process completed")

if __name__ == "__main__":
    main()


"""
usage: clear_records [-h] (-x | -d) [-s [STATIONS ...] | -a] --write-to WRITE_TO

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
