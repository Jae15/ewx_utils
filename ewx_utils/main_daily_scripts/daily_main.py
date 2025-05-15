#!/usr/bin/env python
import os
import sys
import argparse
import traceback
from argparse import Namespace
from datetime import date, datetime, timedelta
import psycopg2
from psycopg2 import OperationalError
from dotenv import load_dotenv
load_dotenv()
ewx_base_path = os.getenv("EWX_BASE_PATH")
sys.path.append(ewx_base_path)
from ewx_utils.ewx_config import ewx_log_file
from ewx_utils.db_files.dbs_configfile import get_ini_section_info
from ewx_utils.daily_validation_checks.daily_validation_utils import process_records
from ewx_utils.db_files.dbs_connection import(
    connect_to_db,
    get_mawn_cursor,
    get_mawnqc_cursor,
    get_qcwrite_cursor,
    create_db_connections
)
from ewx_utils.logs.ewx_utils_logs_config import ewx_unstructured_logger
from ewx_utils.logs.ewx_utils_logs_config import EWXStructuredLogger
from typing import List, Dict, Any, Tuple, Optional

my_logger = EWXStructuredLogger(log_path=EWXStructuredLogger)

def close_connections(connections: Dict[str, Any])->None:
    """
    Close all provided database connections and cursors.

    Parameters:
    Connections: Dict[str, Any]
        Dictionary of connection and cursor objects, where keys are the names of the connections/cursors
        and values are the corresponding connection/cursor objects
    """
    my_logger.info("Closing database connections")
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


def fetch_records(cursor: Any, station: str, begin_date: str, end_date: str)-> List[Dict[str,Any]]:
    """
    Fetch records from specified station for a given date range.

    Parameters:
        cursor: Database cursor object to excecute queries.
        station(str): Name of the table(station) to query.
        begin_date(str): Start date of the query in YYYY-MM-DD format.
        end_date(str): End date of the query in YYYY-MM-DD format.
    
    Returns:
        list: A list of dictionaries containing fectched records
    
    Raises:
        Exception: If the query fails or if any other error occurs.
    """
    query = f"SELECT * FROM {station}_daily WHERE date BETWEEN %s and %s"
    my_logger.error(
        f"Executing query {query} with parameters {begin_date}, {end_date}"
    )
    try:
        cursor.execute(query, (begin_date, end_date))
        records = cursor.fetchall()
        my_logger.error(
            f"Fetched {len(records)} from {station} using {cursor}."
        )
        return [dict(record) for record in records]
    
    except Exception as e:
        my_logger.error(f"Error fetching records from {station}: {e}")
        raise

def get_insert_table_columns(cursor: Any, station: str) -> List[str]:
    """
    Retrieve and log column names from the specified station's table.

    Parameters: 
        cursor (object): Database cursor for executing queries
        station (str): Specified weather station
    
    Returns:
        list: A list of column names or an empty list if an error occurs.
    """
    my_logger.info(f"Processing columns for station {station}")

    # Query to fetch a sample row for from the specified table
    query = f"SELECT * FROM {station}_daily LIMIT 1"
    try:
        if cursor is None:
            my_logger.error("Cursor is not valid.")
            return[]
        # Execute the query to fetch a sample row
        cursor.execute(query)

        # Fetch the cursor description to get column names
        columns = [
            desc[0] for desc in cursor.description
        ] # Accessing the first element of each Tuple

        if not columns:
            my_logger.warning(f"No columns found for station {station}_daily")
            return[]
        
        my_logger.info(f"Fetched columns for table {station}_daily: {columns}")
        return columns
    
    except Exception as e:
        my_logger.error(f"An error occurred: {e}")
        return []

def record_exists(cursor: Any, station: str, record: Dict[str, str])-> bool:
    """
    Check if a record exists for the specified weather station.

    Parameters:
        cursor (object): Database cursor object for executing queries.
        station (str): Specified weather station
        record (list): Record containing 'date' and 'time' to check.
    
    Returns:
        bool: True if record exists, False otherwise
    """
    query = f"SELECT 1 from {station} WHERE date = %s AND time = %s"
    try:
        cursor.execute(query, (record['date'], record['time']))
        return cursor.fetchone() is not None
    
    except Exception as e:
        my_logger.error
        

def get_all_stations_list(cursor: Any) -> List[str]:
    """
    Fetch and return station names from database, excluding variables_daily

    Parameters:
        cursor (object): Database cursor for executing queries.
    
    Returns:
        list (list): List of station names without the _daily suffix
    """
    query = """SELECT table_name FROM information_schema.tables
               WHERE table_schema = 'public' AND table_name like '%daily'
               ORDER BY table_name ASC """
    try:
        cursor.execute(query)
        stations = cursor.fetchall()

        if not stations:
            my_logger.warning(f"No stations found")
            return []
        
        stations_list = [dict(row)["table_name"] for row in stations]
        my_logger.info(f"Fetched stations: {stations_list}")

        filtered_stations_list = [
            station for station in stations_list if station != "variables_daily"
        ]
        cleaned_stations_list = [
            station.replace("_daily", "") for station in filtered_stations_list
        ]
        return cleaned_stations_list
    except Exception as e:
        my_logger.error(f"An error occurred: {e}")
        return []
    
def update_records(cursor: Any, station: str, records: List[Dict[str, Any]])->None:
    """
    Update existing records in the specified station's table.

    Parameters:
    cursor: Any
        Database cursor for executing queries.
    station: str
        Specified weather station.
    records: List[Dict[str, Any]]
        List of records to update, where each record is a dictionary with string keys and values of any type.
    """
    if not records:
        my_logger.error(f"No records to update in {station}.")
        
    try:
        record_keys = [key for key in records[0].keys() if key not in ["date", "time"]]
        if not record_keys:
            my_logger.error(f"No updatable keys found in the records for {station}")
            return

        update_query = (
            f" UPDATE {station}_daily SET"
            + ", ".join([f"{col} = %s" for col in record_keys])
            + "WHERE date = %s AND time = %s"
        )

        for record in records:
            update_values = [record[key] for key in record_keys] + [
               record["date"],
               record["time"], 
            ]
            cursor.execute(update_query, update_values)
        my_logger.error(
            f"Updated {len(records)} from {station} using {cursor}."
        )
        
        my_logger.info(f"Updated {len(records)} records in {station}.")
    except Exception as e:
        my_logger.error(f"Error updating records in {station}: {e}")
        raise 

def insert_records(cursor, table: str, records: List[Dict[str, Any]], record_keys: List[str]) -> None:
    """
    Insert records into the given table using the specified cursor.
    Adds validation to prevent mismatched inserts and handles rollback.
    """
    insert_template = f"""
        INSERT INTO {table}_daily ({", ".join(record_keys)})
        VALUES ({", ".join(["%s"] * len(record_keys))})
    """

    for record in records:
        # Check for missing keys
        for key in record_keys:
            if key not in record:
                print(f" Missing key in record: {key}")

        record_vals = [record.get(key) for key in record_keys]
        print("*********************************")
        print(f"Columns (len{len(record_keys)}): {record_keys}")
        print(f"Values (len{len(record_vals)}): {record_vals}")
        print("*********************************")

        # check: field count matches
        if len(record_keys) != len(record_vals):
            print("Mismatch in column/value count!")
            print(f"Columns ({len(record_keys)}): {record_keys}")
            print(f"Values  ({len(record_vals)}): {record_vals}")
            raise ValueError("Column count does not match value count")

        try:
            cursor.execute(insert_template, record_vals)
            my_logger.error(
            f"Inserted {len(records)} from {table} using {cursor}."
        )
        except Exception as e:
            print(f"INSERT failed for table {table}: {e}")
            cursor.connection.rollback()
            continue  # Skip to next record


def insert_or_update_records(cursor, table: str, records: List[Dict[str, Any]], record_keys: List[str], unique_keys: List[str]) -> None:
    """
    Insert or update records into the table.
    Performs UPSERT logic based on unique constraints.
    """
    update_cols = [f"{key} = EXCLUDED.{key}" for key in record_keys if key not in unique_keys]

    insert_template = f"""
        INSERT INTO {table}_daily ({", ".join(record_keys)})
        VALUES ({", ".join(["%s"] * len(record_keys))})
        ON CONFLICT ({", ".join(unique_keys)})
        DO UPDATE SET {", ".join(update_cols)}
    """

    for record in records:
        # Check for missing keys
        for key in record_keys:
            if key not in record:
                print(f" Missing key in record: {key}")

        record_vals = [record.get(key) for key in record_keys]

        if len(record_keys) != len(record_vals):
            print(" Mismatch in column/value count!")
            print(f"Columns ({len(record_keys)}): {record_keys}")
            print(f"Values  ({len(record_vals)}): {record_vals}")
            raise ValueError("Column count does not match value count")

        try:
            cursor.execute(insert_template, record_vals)
            my_logger.error(
            f"Inserted/updated {len(records)} from {table} using {cursor}."
        )
        except Exception as e:
            print(f" UPSERT failed for table {table}_daily: {e}")
            cursor.connection.rollback()
            continue  # Skip to next record


def commit_and_rollback(connection: Any, station: str, records: List[Dict[str, Any]], record_keys, unique_keys)->None:
    """
    Commit records for a station, rollback on error.

    Parameters:
    connection(object): Database connection
    station(str): Specified weather station
    records(list): Records to insert/update where each record is a dictionary
    """
    try:
        with connection.cursor() as cursor:
            insert_or_update_records(cursor, station,records, record_keys, unique_keys)
            my_logger.info("Inserted or updated records successfully.")
            connection.commit()
    except Exception as e:
        print(f"Exception as {e}")
        # Rollback the transaction in case of an error
        connection.rollback()
        my_logger.error(f"Connection failed and rolled back: {e}")

def get_station_data(cursor: Any)-> Dict[str, Dict[str, Any]]:
    """
    Function to get station data including begin dates, end dates and active status of the stations
    
    Parameters:
    cursor: Any
        Database cursor to execute SQL queries
    
    Returns:
    Dict[str, Dict[str, Any]]
        A dictionary containing station names as keys and their corresponding data(active status, bg_dates, ed_dates)
    """
    my_logger.info("Starting to fetch station data.")

    try:
        # Query all the station names and their active status
        station_status_query = "SELECT station_name, active FROM station_info"
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
        SELECT station_name, bg_date, ed_date FROM daily_info WHERE station_name IN ({station_names_placeholders});
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
                 

def time_defaults(user_begin_date: Optional[str], user_end_date: Optional[str]) -> Tuple[str, str]:
    """
    Set default begin and end dates if not provided.

    Parameters:
    user_begin_date (str, optional): User-provided begin date (format: "YYYY-MM-DD").
    user_end_date (str, optional): User-provided end date (format: "YYYY-MM-DD")

    Returns:
    tuple: A tuple containing the begin and end dates as strings.
    """ 
    try:
        # Set default begin_date to 7 days ago if not provided
        if user_begin_date is None:
            user_begin_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        else:
            # Validate and standardize the format
            user_begin_date = datetime.strptime(user_begin_date, "%Y-%m-%d").strftime("%Y-%m-%d")
        
        # Default end_date is 7 days from begin_date if not provided
        if user_end_date is None:
            # Fix: Set user_end_date, not user_begin_date
            user_end_date = (
                datetime.strptime(user_begin_date, "%Y-%m-%d") + timedelta(days=7)
            ).strftime("%Y-%m-%d")  # Add strftime to convert back to string
        else:
            # Validate and standardize the format
            user_end_date = datetime.strptime(user_end_date, "%Y-%m-%d").strftime("%Y-%m-%d")
        
        # Validate that the begin_date comes before the end_date
        if user_begin_date > user_end_date:
            raise ValueError("Begin date should come before end date.")
        
        return user_begin_date, user_end_date

    except ValueError as ve:
        my_logger.error(f"ValueError occurred: {ve}")  # Fixed variable name from e to ve
        # Return default values to prevent None return
        default_begin = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        default_end = (datetime.now()).strftime("%Y-%m-%d")
        my_logger.info(f"Using default dates: {default_begin} to {default_end}")
        return default_begin, default_end
    
    except Exception as e:
        my_logger.error(f"An error occurred: {e}")
        # Return default values to prevent None return
        default_begin = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        default_end = (datetime.now()).strftime("%Y-%m-%d")
        my_logger.info(f"Using default dates: {default_begin} to {default_end}")
        return default_begin, default_end
    
    finally:
        my_logger.info("time_defaults function execution completed.")


def get_runtime_begin_date(process_begin_date: str, station_info: Dict[str, Dict[str, Any]]) -> Dict[str, str]:
    """
    Calculate the runtime begin date based on process begin date and station data.

    Parameters:
    process_begin_date : str
        The date when the process begins, as a string (format: 'YYYY-MM-DD').
    station_info : Dict[str, Dict[str, Any]]
        A dictionary containing station information, where each key is a station name and each value is a dictionary
        that includes at least a key "bg_date" which is a list of background dates.
    
    Returns:
    Dict[str, str]
        A dictionary mapping station names to their respective runtime begin dates.
    """
    my_logger.info("Calculating runtime begin dates.")

    if process_begin_date is None:
        my_logger.warning("Process begin date is None, returning empty dictionary.")
        return {}

    # Fixed line: Use datetime directly instead of datetime.datetime
    process_date = datetime.strptime(process_begin_date, '%Y-%m-%d').date()
    
    runtime_begin_date = {}
    for station_name, info in station_info.items():
        station_begin_dates = info.get("bg_date", [])
        
        if station_begin_dates:
            # Convert all dates in the list to datetime objects
            station_dates = []
            for date_str in station_begin_dates:
                try:
                    # Fixed: Use datetime directly
                    date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
                    station_dates.append(date_obj)
                except ValueError:
                    my_logger.warning(f"Invalid date format in station {station_name}: {date_str}")
            
            if station_dates:
                # Find the latest date from station_dates
                latest_station_date = max(station_dates)
                
                # Compare with process_begin_date and take the later one
                runtime_date = max(process_date, latest_station_date)
                
                # Convert back to string format
                runtime_begin_date[station_name] = runtime_date.strftime('%Y-%m-%d')
                
                my_logger.debug(
                    f"Station: {station_name}, Runtime Begin Date: {runtime_begin_date[station_name]}"
                )
            else:
                runtime_begin_date[station_name] = process_begin_date
                my_logger.debug(
                    f"Station: {station_name}, No valid dates found, using process begin date: {process_begin_date}"
                )
    
    my_logger.info("Runtime begin dates calculation completed.")
    return runtime_begin_date


def get_runtime_end_date(process_end_date: str, station_info: Dict[str, Dict[str, Any]]) -> Dict[str, str]:
    """
    Calculate runtime end date based on the process_end_date and station_info.

    Parameters:
    process_end_date : str
        The date when the process ends, as a string.
    station_info : Dict[str, Dict[str, Any]]
        A dictionary containing station information, where each key is a station name and each value is a dictionary
        that includes at least keys "ed_date" (a list of end dates) and "active" (a boolean status).

    Returns:
    Dict[str, str]
        A dictionary mapping station names to their respective runtime and end dates.       
    """
    my_logger.info("Calculating runtime end dates.")

    if process_end_date is None:
        my_logger.warning("Process end date is None, returning empty dictionary.")
        return {}
    
    runtime_end_date = {}
    for station_name, info in station_info.items():
        # Fix: Use "ed_date" instead of "end_date" as mentioned in the docstring
        # Also add a get() method with default empty list to avoid KeyError
        station_end_date = info.get("ed_date", [])[0] if info.get("ed_date", []) else None
        
        # Use get() with default False to avoid KeyError if "active" key is missing
        active_status = info.get("active", False)

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
    
    my_logger.info("Runtime end dates calculation completed.")
    return runtime_end_date

def main() -> None:
    """
    Main function to check and update data from daily_main in mawndb_qc.

    Initializes an argument parser for date ranges, execution modes, and database connections. 
    Processes records for specified stations and updates the QC database if execution is requested.
    """
    ini_file_path = "path_to_ini_file.ini"
    
    # Initialize argument parser
    parser = argparse.ArgumentParser(
        prog="daily_main",
        description="Checks data from daily_main in mawndb_qc and adds estimates from RTMA or fivemin data as needed",
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
    group.add_argument(
        "--show-sections",
        action="store_true",
        help="Display available sections in the INI file and exit",
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
        "--station",
        type=str,
        help="Run for a single station",
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
        required=False,
        help="Section names in INI file for reading data (can specify multiple)",
    )
    parser.add_argument(
        "--write-to",
        type=str,
        required=False,
        help="Section name in INI file for writing data",
    )

    args = parser.parse_args()

    # If show-sections flag is set, display section info and exit
    if args.show_sections:
        section_info = get_ini_section_info(ini_file_path)
        print(section_info)
        my_logger.debug(section_info)
        return

    # Handle single station case
    if args.station:
        args.stations = [args.station]

    # Parse and set date ranges
    begin_date, end_date = time_defaults(args.begin, args.end)

    try:
        # Establish database connections based on args
        db_connections = create_db_connections(args)
        print(f"db_connections: {db_connections}")
        
        # Create cursors for connections
        mawn_cursor = get_mawn_cursor(db_connections.get('mawn_connection'), 'mawn')
        qcread_cursor = get_mawnqc_cursor(db_connections.get('mawnqc_connection'), 'mawnqc')
        qcwrite_cursor = get_qcwrite_cursor(db_connections.get('qcwrite_connection'), 'qcwrite')
        
        my_logger.error(f"mawn_cursor: {mawn_cursor}")
        my_logger.error(f"qcread_cursor: {qcread_cursor}")
        my_logger.error(f"qcwrite_cursor: {qcwrite_cursor}")

        # Process records for specified stations
        stations = get_all_stations_list(mawn_cursor) if args.all else args.stations
        
        station_info = get_station_data(mawn_cursor)
        runtime_begin_dates = get_runtime_begin_date(begin_date, station_info)
        runtime_end_dates = get_runtime_end_date(end_date, station_info)

        for station in stations:
            my_logger.info(f"Processing station: {station}")
            
            qc_columns = get_insert_table_columns(qcwrite_cursor, station)
            #print(f"QC Columns: {qc_columns}")
            
            mawndb_records = fetch_records(
                mawn_cursor,
                station,
                runtime_begin_dates[station],
                runtime_end_dates[station],
            )
            #print(f"Mawndb Record: {mawndb_records}")
            
            mawnqc_records = fetch_records(
                qcread_cursor,
                station,
                runtime_begin_dates[station],
                runtime_end_dates[station],
            )
            #print(f"Mawnqc record: {mawnqc_records}")

            # Process and clean the records
            cleaned_records = process_records(
                qc_columns, mawndb_records, mawnqc_records, 
                runtime_begin_dates[station], runtime_end_dates[station]
            )
            #print(f"Cleaned Records: {cleaned_records}")

            # If execution is requested, insert or update records in the QC database
            if args.execute and qcwrite_cursor:
                my_logger.info(f"Executing updates for station {station}")
                commit_and_rollback(
                    db_connections["qcwrite_connection"], station, cleaned_records, qc_columns, unique_keys=["date", "time"]
                )

    except Exception as e:
        my_logger.error(traceback.format_exc())
        my_logger.error(f"An error occurred: {e}")
    finally:
        # Close all database connections
        if 'db_connections' in locals():
            close_connections(db_connections)


if __name__ == "__main__":
    main()

"""

usage: daily_main [-h] [-b BEGIN] [-e END] (-x EXECUTE | -d) [-s [STATIONS ...] | -a]
[-q {mawnqc_test:local,mawnqcl: local,mawnqc:dbh11,mawnqc:supercell}] [--mawn {mawn:dbh11}]

python daily_main.py --begin 2024-01-01 --end 2024-01-02 --station aetna --read-from mawn_dbh11 mawnqc_supercell --write-to mawnqc_test -x --execute

python daily_main.py --begin 2024-01-01 --end 2024-01-02 --station aetna --read-from mawn_dbh11 mawnqc_supercell --write-to mawnqc_test --execute

python daily_main.py --begin 2019-05-08 --end 2019-05-10 --station aetna --read-from mawn_dbh11 mawnqc_dbh11
 --write-to mawnqc_test -x

"""
