#!/usr/bin/env python
import os
import sys
import argparse
from argparse import Namespace
import datetime as datetime
from datetime import datetime, timedelta
from datetime import date
from psycopg2 import OperationalError
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
from ewx_utils.db_files.dbs_configfile import get_db_config
from ewx_utils.hourly_validation_checks.hourly_validation_utils import process_records
from ewx_utils.logs.ewx_utils_logs_config import ewx_utils_logger
from typing import List, Dict, Any, Tuple


# Initialize the logger
my_logger = ewx_utils_logger(log_path=ewx_log_file)


def close_connections(connections: Dict[str, Any]) -> None:
    """
    Close all provided database connections and cursors.

    Parameters:
    connections : Dict[str, Any]
        Dictionary of connection and cursor objects, where keys are the names of the connections/cursors
        and values are the corresponding connection/cursor objects.
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


def fetch_records(cursor: Any, station: str, begin_date: str, end_date: str) -> List[Dict[str, Any]]:
    """
    Fetch records from the specified database table for a given date range.

    Parameters:
        cursor: Database cursor object to execute queries.
        station (str): Name of the table (station) to query.
        begin_date (str): Start date of the query in YYYY-MM-DD format.
        end_date (str): End date of the query in YYYY-MM-DD format.

    Returns:
        list: A list of dictionaries containing the fetched records.

    Raises:
        Exception: If the query fails or another error occurs.
    """
    query = f"SELECT * FROM {station}_hourly WHERE date BETWEEN %s AND %s"
    my_logger.error(
        f"Executing query: {query} with parameters: {begin_date}, {end_date}"
    )
    try:
        cursor.execute(query, (begin_date, end_date))
        my_logger.error(f"Executed fetch records {query}")
        records = cursor.fetchall()
        my_logger.error(
            f"Fetched {len(records)} records from {station} using {cursor}."
        )
        return [dict(record) for record in records]
    except Exception as e:
        my_logger.error(f"Error fetching records from {station}: {e}")
        return []


def get_insert_table_columns(cursor: Any, station: str) -> List[str]:
    """
    Retrieve and log column names from the specified station's table.

    Parameters:
    cursor (object): Database cursor for executing queries.
    station (str): Specified weather station.

    Returns:
    list: List of column names or an empty list if an error occurs.
    """
    my_logger.info(f"Processing station: {station}")

    # Query to fetch a sample row from the specified table
    query = f"SELECT * FROM {station}_hourly LIMIT 1"
    try:
        if cursor is None:
            my_logger.error("Cursor is not valid.")
            return []

        # Execute the query to fetch a sample row
        cursor.execute(query)

        # Fetch the cursor description to get column names
        columns = [
            desc[0] for desc in cursor.description
        ]  # Accessing the first element of each tuple

        if not columns:
            my_logger.warning(f"No columns found for table {station}_hourly.")
            return []

        my_logger.info(f"Fetched columns for table {station}_hourly: {columns}")

        return columns
    except Exception as e:
        my_logger.error(f"An error occurred when getting the insert table columns: {e}")
        return []


def record_exists(cursor: Any, station: str, record: Dict[str, str]) -> bool:
    """
    Check if a record exists for the specified weather station.

    Parameters:
    cursor (object): Database cursor for executing queries.
    station (str): Specified weather station.
    record (dict): Record containing 'date' and 'time' to check.

    Returns:
    bool: True if the record exists, False otherwise.
    """
    query = f"SELECT 1 FROM {station}_hourly WHERE date = %s AND time = %s"
    try:
        cursor.execute(query, (record["date"], record["time"]))

        return cursor.fetchone() is not None
    except Exception as e:
        my_logger.error(f"Error checking existence of record in {station}: {e}")
        raise


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
        my_logger.error(f"An error occurred when getting the stations list: {e}")
        return []

def update_records(cursor: Any, station: str, records: List[Dict[str, Any]]) -> None:
    """
    Update existing records in the specified station's table.

    Parameters:
    cursor : Any
        Database cursor for executing queries.
    station : str
        Specified weather station.
    records : List[Dict[str, Any]]
        List of records to update, where each record is a dictionary with string keys and values of any type.
    """
    if not records:
        my_logger.error(f"No records to update in {station}.")
        return

    try:
        record_keys = [key for key in records[0].keys() if key not in ["date", "time", "id"]]
        if not record_keys:
            my_logger.error(f"No updatable keys found in the records for {station}.")
            return

        update_query = (
            f"UPDATE {station}_hourly SET "
            + ", ".join([f"{col} = %s" for col in record_keys])
            + " WHERE date = %s AND time = %s"
        )

        for record in records:
            update_values = [record[key] for key in record_keys] + [
                record["date"],
                record["time"],
            ]
            cursor.execute(update_query, update_values)

        my_logger.info(f"Updated {len(records)} records in {station}.")
    except Exception as e:
        my_logger.error(f"Error updating records in {station}: {e}")
        raise


def insert_records(cursor: Any, station: str, records: List[Dict[str, Any]]) -> None:
    """
    Insert records into the specified station's table.

    Parameters:
    cursor : Any
        Database cursor for executing queries.
    station : str
        Specified weather station.
    records : List[Dict[str, Any]]
        List of records to insert, where each record is a dictionary with string keys and values of any type.
    """
    if not records:
        my_logger.error(f"No records to insert into {station}.")
        return

    try:
        # Use the first record to get column names
        record_keys = list(records[0].keys())
        my_logger.info(f"Record Keys: {record_keys}")
        # Skip the 'id' column if it exists
        if "id" in record_keys:
            record_keys.remove("id")

        if not record_keys:
            my_logger.error(f"No keys found in the records for {station}.")
            return

    except (IndexError, AttributeError, KeyError) as e:
        my_logger.error(f"Error accessing record keys: {e}")
        return

    # Prepare the INSERT query using only the available columns
    db_columns = ", ".join(record_keys)
    values_placeholder = ", ".join(["%s"] * len(record_keys))
    query = f"INSERT INTO {station}_hourly ({db_columns}) VALUES ({values_placeholder})"

    my_logger.info(f"Constructed INSERT query: {query}")

    try:
        for record in records:
            record_vals = [record[key] for key in record_keys]
            cursor.execute(query, record_vals)
        my_logger.info(f"Inserted {len(records)} records into {station}.")
    except Exception as e:
        if query:
            print(f"query: {query}")
        if "record_vals" in locals():
            print(f"record_vals: {record_vals}")
        my_logger.error(f"Error inserting records into {station}: {e}")
        raise


def insert_or_update_records(cursor: Any, station: str, records: List[Dict[str, Any]]) -> None:
    """
    Insert or update records in the specified station's table.

    Parameters:
    cursor : Any
        Database cursor for executing queries.
    station : str
        Specified weather station.
    records : List[Dict[str, Any]]
        List of records to insert or update, where each record is a dictionary with string keys and values of any type.
    """
    try:
        for record in records:
            if record_exists(cursor, station, record):
                update_records(cursor, station, [record])
            else:
                insert_records(cursor, station, [record])
    except Exception as e:
        my_logger.error(f"Error in insert_or_update operation for {station}: {e}")
        raise
    

def commit_and_rollback(connection: Any, station: str, records: List[Dict[str, Any]]) -> None:
    """
    Commit records for a station; rollback on error.

    Parameters:
    connection (object): Database connection.
    station (str): Specified weather station.
    records (list): Records to insert/update, where each record is a dictionary.
    """
    try:
        with connection.cursor() as cursor:
            insert_or_update_records(cursor, station, records)
        my_logger.info("Inserted/Updated records successfully")
        connection.commit()
        my_logger.info("Successfully committed transaction")
    except Exception as e:
        print(f"Exception as {e}")
        # Rollback the transaction in case of error
        connection.rollback()
        my_logger.error(f"Transaction failed and rolled back: {e}")


def get_station_data(cursor: Any) -> Dict[str, Dict[str, Any]]:
    """
    Function to get station data including begin dates, end dates, and active status of the stations.

    Parameters:
    cursor : Any
        Database cursor to execute SQL queries.

    Returns:
    Dict[str, Dict[str, Any]]
        A dictionary containing station names as keys and their corresponding data (active status, bg_dates, ed_dates).
    """
    my_logger.info("Starting to fetch station data.")

    try:
        # Query all the station names and their active status
        station_status_query = "SELECT station_name, active from station_info"
        cursor.execute(station_status_query)
        station_info_columns = cursor.fetchall()

        station_info_dict = {}
        for row in station_info_columns:
            station_name = row["station_name"]
            station_status = row["active"]
            station_info_dict[station_name] = {
                "active": station_status,
                "bg_date": [],
                "ed_date": [],
            }

        # Prepare placeholders for the query
        station_names_placeholders = ", ".join(
            f"'{name}'" for name in station_info_dict.keys()
        )

        # Query to get bg dates and ed dates for all stations
        date_info_query = f"""
        SELECT station_name, bg_date, ed_date FROM hourly_info WHERE station_name IN ({station_names_placeholders});
        """
        cursor.execute(date_info_query)
        date_info_columns = cursor.fetchall()

        for row in date_info_columns:
            station_name = row["station_name"]
            bg_date = row["bg_date"].strftime("%Y-%m-%d") if row["bg_date"] else None
            ed_date = row["ed_date"].strftime("%Y-%m-%d") if row["ed_date"] else None

            if bg_date:
                station_info_dict[station_name]["bg_date"].append(bg_date)
            if ed_date:
                station_info_dict[station_name]["ed_date"].append(ed_date)

        my_logger.info("Successfully fetched station data.")
        return station_info_dict

    except Exception as e:
        my_logger.error(f"An error occurred while fetching station data: {e}")
        return {}


def time_defaults(user_begin_date: str, user_end_date: str) -> Tuple[str, str]:
    """
    Set default begin and end dates if not provided.

    Parameters:
    user_begin_date (str): User-provided begin date (format: 'YYYY-MM-DD').
    user_end_date (str): User-provided end date (format: 'YYYY-MM-DD').

    Returns:
    tuple: A tuple containing the begin and end dates as strings.
    """
    try:
        if user_begin_date is None:
            user_begin_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        else:
            user_begin_date = datetime.strptime(user_begin_date, "%Y-%m-%d").strftime(
                "%Y-%m-%d"
            )

        # Default end_date is 7 days from begin_date if not provided
        if user_end_date is None:
            user_end_date = (
                datetime.strptime(user_begin_date, "%Y-%m-%d") + timedelta(days=7)
            ).strftime("%Y-%m-%d")
        else:
            user_end_date = datetime.strptime(user_end_date, "%Y-%m-%d").strftime(
                "%Y-%m-%d"
            )

        # Validate that the begin_date comes before the end_date
        if user_begin_date > user_end_date:
            raise ValueError("Begin date should come before end date.")

        return user_begin_date, user_end_date

    except ValueError as ve:
        my_logger.error(f"ValueError occurred when executing the time_defaults function: {ve}")
    except Exception as e:
        my_logger.error(f"An error occurred when executing the time_defaults function: {e}")
    finally:
        my_logger.info("time_defaults function execution completed.")


def get_runtime_begin_date(process_begin_date: str, station_info: Dict[str, Dict[str, Any]]) -> Dict[str, str]:
    """
    Calculate the runtime begin date based on process begin date and station data.

    Parameters:
    process_begin_date : str
        The date when the process begins, as a string.
    station_info : Dict[str, Dict[str, Any]]
        A dictionary containing station information, where each key is a station name and each value is a dictionary
        that includes at least a key "bg_date" which is a list of background dates.

    Returns:
    Dict[str, str]
        A dictionary mapping station names to their respective runtime begin dates.
    """
    my_logger.info("Calculating runtime begin dates.")

    if process_begin_date is None:
        my_logger.warning("Process begin date is None, returning None.")
        return None

    runtime_begin_date = {}
    for station_name, info in station_info.items():
        station_begin_date = info["bg_date"][0] if info["bg_date"] else None
        if station_begin_date:
            runtime_begin_date[station_name] = max(
                process_begin_date, station_begin_date
            )
            my_logger.debug(
                f"Station: {station_name}, Runtime Begin Date: {runtime_begin_date[station_name]}"
            )
        else:
            runtime_begin_date[station_name] = None

    my_logger.info("Runtime begin dates calculation completed.")
    return runtime_begin_date


def get_runtime_end_date(process_end_date: str, station_info: Dict[str, Dict[str, Any]]) -> Dict[str, str]:
    """
    Calculate runtime end date based on the process_end_date and station info.

    Parameters:
    process_end_date : str
        The date when the process ends, as a string.
    station_info : Dict[str, Dict[str, Any]]
        A dictionary containing station information, where each key is a station name and each value is a dictionary
        that includes at least keys "ed_date" (a list of end dates) and "active" (a boolean status).

    Returns:
    Dict[str, str]
        A dictionary mapping station names to their respective runtime end dates.
    """
    my_logger.info("Calculating runtime end dates.")

    if process_end_date is None:
        my_logger.warning("Process end date is None, returning None.")
        return None

    runtime_end_date = {}
    for station_name, info in station_info.items():
        station_end_date = info["ed_date"][0] if info["ed_date"] else None
        active_status = info["active"]

        if active_status and process_end_date == date.today().strftime("%Y-%m-%d"):
            runtime_end_date[station_name] = process_end_date
            my_logger.debug(
                f"Station: {station_name}, Runtime End Date: {runtime_end_date[station_name]}"
            )
        elif station_end_date:
            runtime_end_date[station_name] = min(process_end_date, station_end_date)
            my_logger.debug(
                f"Station: {station_name}, Runtime End Date: {runtime_end_date[station_name]}"
            )
        else:
            runtime_end_date[station_name] = None

    my_logger.info("Runtime end dates calculation completed.")
    return runtime_end_date

def main() -> None:
    """
    Main function to check and update data from hourly_main in mawndb_qc.

    Initializes an argument parser for date ranges, execution modes, and database connections. 
    Processes records for specified stations and updates the QC database if execution is requested.

    Command-line arguments:
    -b, --begin: Start date (no time accepted)
    -e, --end: End date (no time accepted)
    -x, --execute: Execute SQL and change data in QC database
    -d, --dryrun: Do not execute SQL, just write to stdout/store data in test database
    -s, --stations: Run for specific stations (list station names)
    -a, --all: Run for all stations
    -q, --qcwrite: Modify data in a specific database
    --mawn: Read mawndb data from a specific database
    --rtma: Read rtma data from a specific database
    """
    ini_file_path = "path_to_ini_file.ini"
    section_info_help = get_ini_section_info(ini_file_path)
    
    # Initialize argument parser
    parser = argparse.ArgumentParser(
        prog="hourly_main",
        description="Checks data from hourly_main in mawndb_qc and adds estimates from RTMA or fivemin data as needed",
        epilog="Check missing data and ask python scripts for help",
    )
    parser.add_argument("-b", "--begin", type=str, help="Start date (no time accepted)")
    parser.add_argument("-e", "--end", type=str, help="End date (no time accepted)")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "-x",
        "--execute",
        action="store_true",
        help="Execute SQL and change data in QC database",
    )
    group.add_argument(
        "-d",
        "--dryrun",
        action="store_true",
        help="Do not execute SQL, just write to stdout/store data in test database",
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
        "-a", 
        "--all", 
        action="store_true", 
        default=False, 
        help="Run for all stations"
    )

    parser.add_argument(
        "--read-from",
        type=str,
        nargs="+", 
        required=True,
        help="Section names in INI file for reading data (can specify multiple)",
    )
    parser.add_argument(
        "--write-to",
        type=str,
        required=True,
        help="Section name in INI file for writing data",
    )

    args = parser.parse_args()

    # Parse and set date ranges
    begin_date, end_date = time_defaults(args.begin, args.end)

    # Establish only necessary database connections based on args
    db_connections = create_db_connections(args)

    try:
        # Use the necessary connections and cursors based on what is required
        mawn_cursor = get_mawn_cursor(db_connections['mawn_connection'], 'mawn')
        rtma_cursor = get_rtma_cursor(db_connections['rtma_connection'], 'rtma')
        qcwrite_cursor = get_qcwrite_cursor(db_connections['qcwrite_connection'], 'qcwrite')

        # Log cursor status
        my_logger.error(f"mawn_cursor: {mawn_cursor}")
        my_logger.error(f"rtma_cursor: {rtma_cursor}")
        my_logger.error(f"qctest_cursor: {qcwrite_cursor}")

        # Process records for specified stations
        if args.all:
            stations = get_all_stations_list(mawn_cursor)
            #print(f"Stations: {stations}")
        else:
            stations = args.stations

        station_info = get_station_data(mawn_cursor)

        runtime_begin_dates = get_runtime_begin_date(begin_date, station_info)
        runtime_end_dates = get_runtime_end_date(end_date, station_info)

        for station in stations:
            try: 
                qc_columns = get_insert_table_columns(qcwrite_cursor, station)
                my_logger.error("Success fetching qc_columns")
                mawn_records = fetch_records(
                    mawn_cursor,
                    station,
                    runtime_begin_dates[station],
                    runtime_end_dates[station],
                )
                rtma_records = fetch_records(
                    rtma_cursor,
                    station,
                    runtime_begin_dates[station],
                    runtime_end_dates[station],
                )
                my_logger.error("Start process records")

                # Process and clean the records
                cleaned_records = process_records(
                    qc_columns, mawn_records, rtma_records, runtime_begin_dates[station], runtime_end_dates[station]
                )
                my_logger.error("Finish process records")

                # If execution is requested and QC cursor is available, insert or update records in the QC database
                if args.execute and qcwrite_cursor:
                    # Call commit_and_rollback with the operations
                    commit_and_rollback(
                        db_connections["qcwrite_connection"], station, cleaned_records
                    )
            except Exception as e:
                my_logger.error(f"An error occurred when processing {station}")
                print(f"An error occurred when processing {station}")

    except Exception as e:
        my_logger.error(f"An error occurred in main: {e}")
    finally:
        # Close all database connections
        close_connections(db_connections)


if __name__ == "__main__":
    main()


"""
usage: hourly_main [-h] [-b BEGIN] [-e END] (-x | -d) [-s [STATIONS ...] | -a] --read-from READ_FROM [READ_FROM ...] --write-to WRITE_TO

python hourly_main.py -x -s aetna --read-from sample_section01 sample_section02 --write-to sample_section03

python hourly_main.py -x -b 2025-02-08 -e 2025-02-14 -a --read-from sample_section sample_section01 --write-to sample_section02

python hourly_main.py -x -s aetna --read-from mawn_dbh11 rtma_dbh11 --write-to mawnqc_test

python hourly_main.py -x -a --read-from mawn_dbh11 rtma_dbh11 --write-to mawnqc_test

python hourly_main.py -x -b 2025-03-11 -e 2025-03-11 -a --read-from mawn_dbh11 rtma_dbh11 --write-to mawnqc_test

"""
