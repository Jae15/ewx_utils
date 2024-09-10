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
# Import process_records directly for handling validation and cleanin
from validation_checks.mawndbsrc import (
    process_records
) 
from ewxutils_logsconfig import ewx_utils_logger

# Initialize the logger
my_logger = ewx_utils_logger()

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

    parser.add_argument('-q', '--qcwrite', type=str, default='mawnqc_test:local', choices=['mawnqc_test:local', 'mawnqcl:local', 'mawnqc:dbh11'],
                        help='Modify data in a specific database')
    parser.add_argument("--mawn", type=str, choices=['mawn_dbh11:dbh11'], default='mawn:dbh11',
                        help='Read mawndb data from a specific database')
    parser.add_argument("--rtma", type=str, choices=['rtma_dbh11:dbh11'], default='rtma_dbh11:dbh11',
                        help='Read rtma data from a specific database')

    args = parser.parse_args()
    print(args)
    begin_date, end_date = time_defaults(args.begin, args.end)

def create_db_connections():
    """
    Create and return database connections and cursors for mawndb, mawndbqc, rtma, and qctest databases.
    """
    ewx_utils_logger.error("Creating database connections.")
    try:
        # Connect to various databases and get cursor objects
        mawn_dbh11_connection = connect_to_mawn_dbh11()
        mawn_dbh11_cursor = mawn_dbh11_cursor_connection(mawn_dbh11_connection)

        mawn_supercell_connection = connect_to_mawn_supercell()
        mawn_supercell_cursor = mawn_supercell_cursor_connection(mawn_supercell_connection)

        mawnqc_dbh11_connection = connect_to_mawnqc_dbh11()
        mawnqc_dbh11_cursor = mawnqc_dbh11_cursor_connection(mawnqc_dbh11_connection)

        mawnqc_supercell_connection = connect_to_mawnqc_supercell()
        mawnqc_supercell_cursor = mawnqc_supercell_cursor_connection(mawnqc_supercell_connection)

        rtma_dbh11_connection = connect_to_rtma_dbh11()
        rtma_dbh11_cursor = rtma_dbh11_cursor_connection(rtma_dbh11_connection)

        rtma_supercell_connection = connect_to_rtma_supercell()
        rtma_supercell_cursor = rtma_supercell_cursor_connection(rtma_supercell_connection)

        qctest_connection = connect_to_mawnqc_test()
        qctest_cursor = mawnqc_test_cursor_connection(qctest_connection)

        my_logger.error("Database connections created successfully.")
        return (
            mawn_dbh11_connection,
            mawn_dbh11_cursor,
            mawnqc_dbh11_connection,
            mawnqc_dbh11_cursor,
            rtma_dbh11_connection,
            rtma_supercell_cursor,
            qctest_connection,
            qctest_cursor,
        )
    except Exception as e:
        my_logger.error(f"Error creating database connections: {e}")
        raise

def fetch_records(cursor, table_name, date):
    """
    Fetch records from the specified table and date.
    """
    query = f"SELECT * FROM {table_name} WHERE date = '{date}'"
    my_logger.error(f"Executing query: {query}")
    try:
        cursor.execute(query)
        records = cursor.fetchall()
        my_logger.error(f"Fetched {len(records)} records from {table_name} for date {date}.")
        return [dict(record) for record in records]
    except Exception as e:
        my_logger.error(f"Error fetching records from {table_name} for date {date}: {e}")
        raise

def insert_records(cursor, table_name, records):
    """
    Insert records into the specified table.
    """
    if not records:
        my_logger.error(f"No records to insert into {table_name}.")
        return

    try:
        record_keys = list(records[0].keys())
    except (IndexError, AttributeError, KeyError) as e:
        my_logger.error(f"Error accessing record keys: {e}")
        return

    db_columns = ", ".join(record_keys)
    qc_values = ", ".join(["%s"] * len(record_keys))

    query = f"INSERT INTO {table_name} ({db_columns}) VALUES ({qc_values})"
    my_logger.error(f"Constructed INSERT query: {query}")

    try:
        for record in records:
            record_vals = list(record.values())
            cursor.execute(query, record_vals)
        my_logger.error(f"Inserted {len(records)} records into {table_name}.")
    except Exception as e:
        my_logger.error(f"Error inserting records into {table_name}: {e}")
        raise

def commit_and_close(conn):
    """
    Commit the transaction and close the database connection.
    """
    try:
        conn.commit()
        conn.close()
        my_logger.error("Transaction committed and connection closed.")
    except Exception as e:
        my_logger.error(f"Error committing transaction or closing connection: {e}")
        raise

def time_defaults(user_begin_date: str, user_end_date: str):
    try:
        if user_begin_date is None:
            user_begin_date = datetime.now() - timedelta(days=7)
        else:
            user_begin_date = datetime.strptime(user_begin_date, '%Y-%m-%d')

        if user_end_date is None:
            user_end_date = user_begin_date + timedelta(days=7)
        else:
            user_end_date = datetime.strptime(user_end_date, '%Y-%m-%d')

        if user_begin_date > user_end_date:
            raise ValueError("User begin date should come before user end date")

        return user_begin_date, user_end_date
    except ValueError as ve:
        my_logger.error("ValueError occurred: %s", ve)
        print("ValueError:", ve)
    except Exception as e:
        my_logger.error("An error has occurred: %s", exc_info=e)
        print("An error has occurred", e)
    finally:
        my_logger.error("Function execution completed")

if __name__ == "__main__":
    main()

    """
    python hourly_main.py --begin "2023-01-01" --end "2023-01-07" -q mawnqc_test:local -x
     
    """
