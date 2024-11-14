#!/usr/bin/env python
import os
import sys
import argparse
import datetime as datetime
from datetime import datetime, timedelta
from psycopg2 import OperationalError
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

load_dotenv()
# Initialize the logger
my_logger = ewx_utils_logger(log_path = ewx_log_file)

# Function to create necessary database connections based on user-specified arguments
def create_db_connections(args):
    """
    Creating and returning only the necessary database connections and cursors for mawndb, rtma, qctest, etc.,
    based on the user-specified arguments.
    """
    my_logger.info("Creating only necessary database connections.")
    connections = {}

    try:
        # Connect to mawn database only if it's specified
        if args.mawn == 'mawn:dbh11':
            my_logger.info("Connecting to MAWN database (dbh11).")
            connections['mawn_dbh11_connection'] = connect_to_mawn_dbh11()
            connections['mawn_dbh11_cursor'] = mawn_dbh11_cursor_connection(connections['mawn_dbh11_connection'])

        # Connect to rtma database only if it's specified
        if args.rtma == 'rtma:dbh11':
            my_logger.info("Connecting to RTMA database (dbh11).")
            connections['rtma_dbh11_connection'] = connect_to_rtma_dbh11()
            connections['rtma_dbh11_cursor'] = rtma_dbh11_cursor_connection(connections['rtma_dbh11_connection'])

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

        my_logger.info("Database connections created successfully based on the required databases.")
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
    Fetch records from the specified station between begin_date and end_date.
    """
    query = f"SELECT * FROM {station} WHERE date BETWEEN %s AND %s"
    my_logger.error(f"Executing query: {query} with parameters: {begin_date}, {end_date}")
    try:
        cursor.execute(query, (begin_date, end_date))
        records = cursor.fetchall()
        my_logger.error(f"Fetched {len(records)} records from {station}.")
        return [dict(record) for record in records]
    except Exception as e:
        my_logger.error(f"Error fetching records from {station}: {e}")
        raise


def get_insert_table_columns(cursor, station):
    """
    Fetch column names of the specified table and log them.
    """
    query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = %s AND TABLE_SCHEMA = 'public' "
    try:
        if cursor is None:
            my_logger.error("Cursor is not valid.")
            return []

        cursor.execute(query, (station,))
        columns = cursor.fetchall()
        #print(columns)

        if not columns:
            my_logger.warning(f"No columns found for table {station}.")
            return []

        # Creating a list of column names and logging them
        columns_list = [row['column_name'] for row in columns]
        my_logger.info(f"Fetched columns for table {station}: {columns_list}")

        # Print and/ log the retrieved QC columns for verification
        #print("QC Columns:", columns_list)  
        my_logger.info(f"QC Columns retrieved: {columns_list}")

        return columns_list

    except Exception as e:
        my_logger.error(f"Error fetching columns for table {station}: {e}")
        raise


def filter_records_by_columns(records, qc_columns):
    """
    Filter the records to only include the QC columns.
    """
    if not qc_columns:
        my_logger.error("No QC columns provided for filtering.")
        return []

    filtered_records = [
        {k: v for k, v in record.items() if k in qc_columns}
        for record in records
    ]
    #print(filtered_records)
    
    my_logger.info(f"Filtered records to include columns: {qc_columns}")
    return filtered_records

def record_exists(cursor, station, record):
    """
    Check if a record exists in the table for the specific station based on the date and time
    """
    query = f"SELECT 1 FROM {station} WHERE date = %s AND time = %s"

    try:
        cursor.execute(query, (record['date'], record['time']))
        return cursor.fetchone() is not None
    except Exception as e:
        my_logger.error(f"Error checking existece of record in {station}: {e}")
        raise

def update_records(cursor, station, records):
    """
    Update existing records in the specified table.
    """
    if not records:
        my_logger.error(f"No records to update in {station}.")
        return

    try:
        qc_columns = get_insert_table_columns(cursor, station)
        if not qc_columns:
            my_logger.error(f"No valid columns found for table {station}. Cannot proceed with update.")
            return

        filtered_records = filter_records_by_columns(records, qc_columns)
        if not filtered_records:
            my_logger.error(f"No valid columns to update for table {station}.")
            return

        record_keys = [key for key in filtered_records[0].keys() if key not in ['date', 'time']]
        if not record_keys:
            my_logger.error(f"No updatable keys found in the filtered records for {station}.")
            return

        update_query = f"UPDATE {station} SET " + ", ".join([f"{col} = %s" for col in record_keys]) + " WHERE date = %s AND time = %s"
        
        for record in filtered_records:
            update_values = [record[key] for key in record_keys] + [record['date'], record['time']]
            cursor.execute(update_query, update_values)

        my_logger.info(f"Updated {len(filtered_records)} records in {station}.")
    except Exception as e:
        my_logger.error(f"Error updating records in {station}: {e}")
        raise

def insert_records(cursor, station, records):
    """
    Insert records into the specified table, filtered by the table's columns.
    """
    if not records:
        my_logger.error(f"No records to insert into {station}.")
        return
    try:
        # Fetch allowed columns for the insert table
        qc_columns = get_insert_table_columns(cursor, station)

        if not qc_columns:
            my_logger.error(f"No valid columns found for table {station}. Cannot proceed with insert.")
            return

        # Filter the records by the allowed columns
        filtered_records = filter_records_by_columns(records, qc_columns)
        my_logger.info(f"Filtered Records: {filtered_records}")

        if not filtered_records:
            my_logger.error(f"No valid columns to insert after filtering for table {station}.")
            return

        # Use the first filtered record to get column names
        record_keys = list(filtered_records[0].keys())
        my_logger.info(f"Record Keys: {record_keys}")

        # Skip the 'id' column if it exists
        if 'id' in record_keys:
            record_keys.remove('id')

        if not record_keys:
            my_logger.error(f"No keys found in the filtered records for {station}.")
            return

    except (IndexError, AttributeError, KeyError) as e:
        my_logger.error(f"Error accessing record keys: {e}")
        return

    # Prepare the INSERT query using only the filtered columns
    db_columns = ", ".join(record_keys)
    qc_values = ", ".join(["%s"] * len(record_keys))
    query = f"INSERT INTO {station} ({db_columns}) VALUES ({qc_values})"

    my_logger.error(f"Constructed INSERT query: {query}")
    
    try:
        for record in filtered_records:
            record_vals = [record[key] for key in record_keys]
            cursor.execute(query, record_vals)
        my_logger.error(f"Inserted {len(filtered_records)} records into {station}.")
    except Exception as e:
        my_logger.error(f"Error inserting records into {station}: {e}")
        raise
    
def insert_or_update_records(cursor, station, records):
    """
    Insert or update records based on existence in the table
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

def time_defaults(user_begin_date: str, user_end_date: str):
    """
    Set default begin and end dates if not provided, and ensure the dates are returned as strings.
    """
    try:
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
        #print("ValueError:", ve)
    except Exception as e:
        my_logger.error(f"An error occurred: {e}")
        #print("An error occurred:", e)
    finally:
        my_logger.info("time_defaults function execution completed.")

def main():
    # Initialize argument parser
    parser = argparse.ArgumentParser(
        prog='hourly_main',
        description='Checks data from hourly_main in mawndb_qc and adds estimates from RTMA or fivemin data as needed',
        epilog='Check missing data and ask python scripts for help'
    )
    parser.add_argument('-b', '--begin', type=str, help='Start date (no time accepted)')
    parser.add_argument('-e', '--end', type=str, help='End date (no time accepted)')
    # parser.add_argument('-f', '--forcedelete', action='store_true', help="Force delete old records")
    # parser.add_argument('-c', '--clearoverride', action='store_true', help="Clear override active flag")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-x', '--execute', action='store_true', help='Execute SQL and change data in QC database')
    group.add_argument('-d', '--dryrun', action='store_true', help='Do not execute SQL, just write to stdout/store data in test database')

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

    # Establish only necessary database connections based on args
    db_connections = create_db_connections(args)

    try:
        # Use the necessary connections and cursors based on what is required
        mawn_cursor = db_connections.get('mawn_dbh11_cursor')
        rtma_cursor = db_connections.get('rtma_dbh11_cursor')
        qc_cursor = db_connections.get('qctest_cursor') or db_connections.get('mawnqcl_cursor') or \
                    db_connections.get('mawnqc_dbh11_cursor') or db_connections.get('mawnqc_supercell_cursor')

        # Process records for the specified stations
        if args.all:
            stations = get_all_stations()  # Fetch all station names
        else:
            stations = args.stations

        for station in stations:
            mawn_records = fetch_records(mawn_cursor, station, begin_date, end_date) if mawn_cursor else []
            rtma_records = fetch_records(rtma_cursor, station, begin_date, end_date) if rtma_cursor else []

            # Process and clean the records
            cleaned_records = process_records(mawn_records, rtma_records, begin_date, end_date)

            """
             # Insert cleaned records if execution mode is set
            if args.execute and qc_cursor:
                insert_records(qc_cursor, station, cleaned_records)
            """
            # If execution is requested and QC cursor is available, insert or update records in the QC database
            if args.execute and qc_cursor:
                insert_or_update_records(qc_cursor, station, cleaned_records)

    finally:
        # Commit and close only the open connections
        for conn_key, conn in db_connections.items():
            if 'connection' in conn_key:
                commit_and_close(conn)
        close_connections(db_connections)

if __name__ == "__main__":
    main()


"""
python hourly_main.py --begin 2023-1-1 --end 2023-1-7 --station aetna_hourly -x

hourly_main [-h] [-b BEGIN] [-e END] [-f] [-c] (-x | -d) [-l] [-s [STATIONS ...] | -a]
[-q {mawnqc_test:local,mawnqcl:local,mawnqc:dbh11,mawnqc:supercell}] [--mawn {mawn:dbh11}] [--rtma {rtma:dbh11}]

"""