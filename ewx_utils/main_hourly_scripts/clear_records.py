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
from ewx_utils.db_files.dbs_configfile import get_ini_section_info
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
        my_logger.info(f"Fetched stations: {stations_list}")

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
    """
    Main function to clear records from specified stations in the QC database.
    
    Command-line arguments:
    -x, --execute: Execute SQL and clear data in QC database
    -d, --dryrun: Do not execute SQL, just log the actions
    -s, --stations: Run for specific stations (list station names)
    -a, --all: Run for all stations
    --write-to: Section name in INI file for the QC database to write to
    """
    section_info_help = get_ini_section_info("config.ini")
    
    parser = argparse.ArgumentParser(
        prog="clear_records",
        description="Clear records from specified stations in the QC database.",
        epilog=section_info_help
    )

    # Execution mode group
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

    # Station selection group
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "-s",
        "--stations",
        nargs="*",
        type=str,
        help="Run for specific stations (list station names)",
    )
    group.add_argument(
        "-a", 
        "--all", 
        action="store_true", 
        default=False, 
        help="Run for all stations"
    )

    # Database section argument
    parser.add_argument(
        "--write-to",
        type=str,
        required=True,
        help="Section name in INI file for the QC database to write to",
    )

    args = parser.parse_args()
    my_logger.info(f"Parsed Arguments: {args}")

    # Establish database connections based on args
    db_connections = create_db_connections(args)
    my_logger.info(f"Database Connections: {db_connections}")

    try:
        # Use the necessary cursor for the QC database
        write_cursor = db_connections.get("write_connection").cursor()
        my_logger.info(f"Write cursor obtained: {write_cursor}")
        
        if args.all:
            stations = get_all_stations_list(write_cursor)
            my_logger.info(f"Clearing all stations: {stations}")
        else:
            stations = args.stations
            my_logger.info(f"Clearing specific stations: {stations}")

        # Clear records for specified stations
        for station in stations:
            my_logger.info(f"Processing station: {station}")
            if args.execute:
                clear_and_commit(db_connections["write_connection"], station)
                my_logger.info(f"Cleared records for station {station}")
            else:
                my_logger.info(f"Dry run: Would clear records for station {station}")

    except Exception as e:
        my_logger.error(f"An error occurred: {e}")
        raise
    finally:
        # Close all database connections
        close_connections(db_connections)
        my_logger.info("Database connections closed")


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
