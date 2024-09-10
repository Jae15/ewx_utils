import sys
import os

# Add the directory containing the `ewx_utils` module to the Python path
sys.path.append("c:/Users/mwangija/data_file/ewx_utils/ewx_utils")
from .mawndbsrc import process_records

# Import database connection functions
from db_files.dbconnection import (
    connect_to_mawn_dbh11,
    connect_to_mawn_supercell,
    connect_to_mawnqc_dbh11,
    connect_to_mawnqc_supercell,
    connect_to_mawnqcl,
    connect_to_rtma_dbh11,
    connect_to_rtma_supercell,
    connect_to_mawnqc_test,
)

# Import database cursor connection functions
from db_files.dbconnection import (
    mawn_dbh11_cursor_connection,
    mawn_supercell_cursor_connection,
    mawnqc_dbh11_cursor_connection,
    mawnqc_supercell_cursor_connection,
    mawnqcl_cursor_connection,
    rtma_dbh11_cursor_connection,
    rtma_supercell_cursor_connection,
    mawnqc_test_cursor_connection,
)

# Import the logger configuration
from dryrunlogs_config import dryrun_logger

# Import necessary functions from the `mawndbsrc` module
from validation_checks.mawndbsrc import (
    combined_datetime,
    creating_mawnsrc_record,
    relh_cap,
    replace_none_with_rtmarecord,
    process_records,
)

# Import required variables list
from variables_list import relh_vars

# Initialize the logger
logger = dryrun_logger()

def create_db_connections():
    """
    Create and return database connections and cursors for mawndb, mawndbqc, rtma, and qctest databases.
    """
    logger.info("Creating database connections.")
    try:
        # Connect to various databases and get cursor objects
        mawn_dbh11_conn = connect_to_mawn_dbh11()
        mawn_dbh11_cur = mawn_dbh11_cursor_connection(mawn_dbh11_conn)

        mawn_supercell_conn = connect_to_mawn_supercell()
        mawn_supercell_cur = mawn_supercell_cursor_connection(mawn_supercell_conn)

        mawnqc_dbh11_conn = connect_to_mawnqc_dbh11()
        mawnqc_dbh11_cur = mawnqc_dbh11_cursor_connection(mawnqc_dbh11_conn)

        mawnqc_supercell_conn = connect_to_mawnqc_supercell()
        mawnqc_supercell_cur = mawnqc_supercell_cursor_connection(mawnqc_supercell_conn)

        rtma_dbh11_conn = connect_to_rtma_dbh11()
        rtma_dbh11_cur = rtma_dbh11_cursor_connection(rtma_dbh11_conn)

        rtma_supercell_conn = connect_to_rtma_supercell()
        rtma_supercell_cur = rtma_supercell_cursor_connection(rtma_supercell_conn)

        qctest_conn = connect_to_mawnqc_test()
        qctest_cur = mawnqc_test_cursor_connection(qctest_conn)

        logger.info("Database connections created successfully.")
        return (
            mawn_dbh11_conn,
            mawn_dbh11_cur,
            mawnqc_dbh11_conn,
            mawnqc_dbh11_cur,
            rtma_dbh11_conn,
            rtma_supercell_cur,
            qctest_conn,
            qctest_cur,
        )
    except Exception as e:
        logger.error(f"Error creating database connections: {e}")
        raise

def fetch_records(cursor, table_name, date):
    """
    Fetch records from the specified table and date.
    """
    query = f"SELECT * FROM {table_name} WHERE date = '{date}'"
    logger.info(f"Executing query: {query}")
    try:
        cursor.execute(query)
        records = cursor.fetchall()
        logger.info(f"Fetched {len(records)} records from {table_name} for date {date}.")
        return [dict(record) for record in records]
    except Exception as e:
        logger.error(f"Error fetching records from {table_name} for date {date}: {e}")
        raise

def insert_records(cursor, table_name, records):
    """
    Insert records into the specified table.
    """
    if not records:
        logger.warning(f"No records to insert into {table_name}.")
        return

    try:
        record_keys = list(records[0].keys())
    except (IndexError, AttributeError, KeyError) as e:
        logger.error(f"Error accessing record keys: {e}")
        return

    db_columns = ", ".join(record_keys)
    qc_values = ", ".join(["%s"] * len(record_keys))

    query = f"INSERT INTO {table_name} ({db_columns}) VALUES ({qc_values})"
    logger.info(f"Constructed INSERT query: {query}")

    try:
        for record in records:
            record_vals = list(record.values())
            cursor.execute(query, record_vals)
        logger.info(f"Inserted {len(records)} records into {table_name}.")
    except Exception as e:
        logger.error(f"Error inserting records into {table_name}: {e}")
        raise

def commit_and_close(conn):
    """
    Commit the transaction and close the database connection.
    """
    try:
        conn.commit()
        conn.close()
        logger.info("Transaction committed and connection closed.")
    except Exception as e:
        logger.error(f"Error committing transaction or closing connection: {e}")
        raise

def main():
    """
    Main function to handle database operations.
    """
    logger.info("Starting main function.")

    # Establish database connections
    try:
        (
            mawndb_conn,
            mawndb_cur,
            mawnqc_conn,
            mawnqc_cur,
            rtma_conn,
            rtma_cur,
            qctest_conn,
            qctest_cur,
        ) = create_db_connections()
    except Exception as e:
        logger.critical("Failed to create database connections. Exiting.")
        return

    # Define table name and date for querying
    table_name = "aetna_hourly"
    date = "2019-08-18"  # The same date for both mawndb and rtma

    # Fetch records from mawndb and rtma
    try:
        mawn_records = fetch_records(mawndb_cur, table_name, date)
        rtma_records = fetch_records(rtma_cur, table_name, date)
    except Exception as e:
        logger.critical("Failed to fetch records. Exiting.")
        return

    # Process records and get clean_records
    try:
        clean_records = process_records(mawn_records, rtma_records)
        logger.info(f"Cleaned Records: {clean_records}")
    except Exception as e:
        logger.error(f"Error processing records: {e}")
        return

    # Insert clean records into qctest
    try:
        insert_records(qctest_cur, table_name, clean_records)
    except Exception as e:
        logger.error("Failed to insert records. Exiting.")
        return

    # Commit transactions and close connections
    try:
        commit_and_close(mawndb_conn)
        commit_and_close(mawnqc_conn)
        commit_and_close(rtma_conn)
        commit_and_close(qctest_conn)
    except Exception as e:
        logger.error("Failed to commit and close connections.")

    logger.info("Main function completed.")

if __name__ == "__main__":
    main()



"""
Pseudo code:
Remember to think through a function that checks for non-exsitent MAWN records (eg data missing on certain dates) and replaces the missing MAWN records (from those dates)with RTMA records
to avoid a case of skipping hours like in the case of date = '2023-04-27' with MAWN 22 records in aetna_hourly table
and 24 records in the RTMA aetna_hourly - Done

Check timeloop script for hints. Do you want to loop through date and time in the time loop script rather than the mawn records.

In my functions above... before checking for record in mawndb_records...I need to check for datetime in datetime list...
And I can obtain this from the timeloop script...

Thought process: 08/15/2024
- Create script that examines whether the records retrieved in the dry_run match the qc tables records 

"""
