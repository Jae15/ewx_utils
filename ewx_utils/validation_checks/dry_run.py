import sys

sys.path.append("c:/Users/mwangija/data_file/ewx_utils/ewx_utils")
from db_files.dbconnection import (
    connect_to_mawndb,
    connect_to_mawndbqc,
    connect_to_rtma,
    connect_to_qctest,
)
from db_files.dbconnection import (
    mawndb_cursor_connection,
    mawnqc_cursor_connection,
    rtma_cursor_connection,
    qctest_cursor_connection,
)
from dryrunlogs_config import dryrun_logger  # Import the logger configuration
from validation_checks.mawndbsrc import (
    combined_datetime,
    creating_mawnsrc_record,
    relh_cap,
    replace_none_with_rtmarecord,
)
from variables_list import relh_vars  # Import required variables list

# Initialize the logger
logger = dryrun_logger()


def create_db_connections():
    """
    Create and return database connections and cursors for mawndb, mawndbqc, rtma, and qctest databases.
    """
    logger.info("Creating database connections.")
    try:
        mawndb_conn = connect_to_mawndb()
        mawndb_cur = mawndb_cursor_connection(mawndb_conn)

        mawnqc_conn = connect_to_mawndbqc()
        mawnqc_cur = mawnqc_cursor_connection(mawnqc_conn)

        rtma_conn = connect_to_rtma()
        rtma_cur = rtma_cursor_connection(rtma_conn)

        qctest_conn = connect_to_qctest()
        qctest_cur = qctest_cursor_connection(qctest_conn)

        logger.info("Database connections created successfully.")
        return (
            mawndb_conn,
            mawndb_cur,
            mawnqc_conn,
            mawnqc_cur,
            rtma_conn,
            rtma_cur,
            qctest_conn,
            qctest_cur,
        )
    except Exception as e:
        logger.error(f"Error creating database connections: {e}")
        raise


def fetch_records(cursor, table_name, date):
    """
    Fetch records from the specified table and date.

    Args:
        cursor: Database cursor object.
        table_name: Name of the table to query.
        date: Date for which records are to be fetched.

    Returns:
        List of records.
    """
    query = f"SELECT * FROM {table_name} WHERE date = '{date}'"
    logger.info(f"Executing query: {query}")
    try:
        cursor.execute(query)
        records = cursor.fetchall()
        logger.info(
            f"Fetched {len(records)} records from {table_name} for date {date}."
        )
        return [dict(record) for record in records]
    except Exception as e:
        logger.error(f"Error fetching records from {table_name} for date {date}: {e}")
        raise


def insert_records(cursor, table_name, records):
    """
    Insert records into the specified table.

    Args:
        cursor: Database cursor object.
        table_name: Name of the table to insert records into.
        records: List of records to insert.
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
    print(f"Instert Query: {query}")

    try:
        for record in records:
            record_vals = list(record.values())
            print(cursor.mogrify(query, record_vals))
            cursor.execute(query, record_vals)
        logger.info(f"Inserted {len(records)} records into {table_name}.")
    except Exception as e:
        logger.error(f"Error inserting records into {table_name}: {e}")
        raise


def commit_and_close(conn):
    """
    Commit the transaction and close the database connection.

    Args:
        conn: Database connection object.
    """
    try:
        conn.commit()
        conn.close()
        logger.info("Transaction committed and connection closed.")
    except Exception as e:
        logger.error(f"Error committing transaction or closing connection: {e}")
        raise


def process_records(mawn_records, rtma_records):
    """
    Process and clean the records by utilizing the validation logic.

    Args:
        mawn_records: List of records from mawndb.
        rtma_records: List of records from rtma.

    Returns:
        List of cleaned records.
    """
    clean_records = []

    for record in mawn_records:
        combined_date = combined_datetime(record)
        id_col_list = ["year", "day", "hour", "rpt_time", "date", "time", "id"]

        mawnsrc_record = creating_mawnsrc_record(record, combined_date, id_col_list)
        mawnsrc_record = relh_cap(mawnsrc_record, relh_vars)

        rtma_record = None
        for rt in rtma_records:
            if (
                rt["date"] == record["date"]
                and rt["time"] == record["time"]
                and rt["hour"] == record["hour"]
            ):
                rtma_record = rt
                break

        if rtma_record:
            mawnsrc_record = replace_none_with_rtmarecord(
                mawnsrc_record, rtma_record, combined_date
            )

        clean_records.append(mawnsrc_record)

    return clean_records


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
    date = "2023-04-27"  # The same date for both mawndb and rtma

    # Fetch records from mawndb and rtma
    try:
        mawn_records = fetch_records(mawndb_cur, table_name, date)
        print(f"lNumber Mawn_records: {len(mawn_records)}")
        rtma_records = fetch_records(rtma_cur, table_name, date)
        print(f"Number rtma_records: {len(rtma_records)}")
    except Exception as e:
        logger.critical("Failed to fetch records. Exiting.")
        return

    # Print fetched records
    logger.info(f"MAWNDB Records: {mawn_records}")
    logger.info(f"RTMA Records: {rtma_records}")

    # Process records and get clean_records
    try:
        clean_records = process_records(
            mawn_records, rtma_records
        )  # Process records using the integrated logic
        logger.info(f"Cleaned Records: {clean_records}")
    except Exception as e:
        logger.error(f"Error processing records: {e}")
        return

    # Insert clean records into qctest
    try:
        insert_records(qctest_cur, "aetna_hourly", clean_records)
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
Remember to think through a function that checks for non-exsitent MAWN records and creates fake MAWN records from RTMA records
to avoid a case of skipping hours like in the case of date = '2023-04-27' with MAWN 22 records in aetna_hourly table
and 24 records in the RTMA aetna_hourly

Check timeloop script for hints. Do you want to loop through date and time in the time loop script rather than the mawn records.

In my functions above... before checking for record in mawndb_records...I need to check for datetime in datetime list...
And I can obtain this from the timeloop script...

"""
