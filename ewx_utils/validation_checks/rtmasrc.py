import ewx_utils.ewx_config as ewx_config 
from logs.ewx_utils_logs_config import ewx_utils_logger
from typing import List, Dict, Any, Optional
import psycopg2.extras
from db_files.dbconnection import connect_to_rtma, rtma_cursor_connection
from validation_logsconfig import validations_logger

# Initialize the logger
my_validation_logger = ewx_utils_logger(path = ewx_config.ewx_log_file)


def connect_to_rtma_db():
    """
    Connects to the RTMA database.

    Returns:
        Optional[psycopg2.extensions.connection]: Connection object for the RTMA database or None if connection fails.
    """
    try:
        connection = connect_to_rtma()
        if connection:
            my_validations_logger.info("Successfully connected to RTMA database.")
            return connection
    except Exception as e:
        my_validations_logger.error(f"Failed to connect to RTMA database: {e}")
    return None


def get_rtma_cursor(connection: psycopg2.extensions.connection):
    """
    Gets a cursor for the RTMA database.

    Args:
        connection (psycopg2.extensions.connection): Connection object for the RTMA database.

    Returns:
        Optional[psycopg2.extras.RealDictCursor]: Cursor object for the RTMA database or None if cursor creation fails.
    """
    try:
        cursor = rtma_cursor_connection(connection)
        if cursor:
            my_validations_logger.info("Successfully obtained RTMA cursor.")
            return cursor
    except Exception as e:
        my_validations_logger.error(f"Failed to get RTMA cursor: {e}")
    return None


def fetch_rtma_records(
    cursor: psycopg2.extras.RealDictCursor, table: str, date: str
) -> List[Dict[str, Any]]:
    """
    Fetches RTMA records for a given table and date.

    Args:
        cursor (psycopg2.extras.RealDictCursor): Cursor object for the RTMA database.
        table (str): Table name.
        date (str): Date in 'YYYY-MM-DD' format.

    Returns:
        List[Dict[str, Any]]: List of records from the RTMA database.
    """
    try:
        query = f"SELECT * FROM {table} WHERE date = %s"
        cursor.execute(query, (date,))
        records = cursor.fetchmany(24)
        my_validations_logger.info(
            f"Successfully fetched RTMA records for table {table} on date {date}."
        )
        return records
    except Exception as e:
        my_validations_logger.error(f"Failed to fetch RTMA records: {e}")
        return []


def close_rtma_connection(
    connection: psycopg2.extensions.connection, cursor: psycopg2.extras.RealDictCursor
) -> None:
    """
    Closes the RTMA database connection and cursor.

    Args:
        connection (psycopg2.extensions.connection): Connection object for the RTMA database.
        cursor (psycopg2.extras.RealDictCursor): Cursor object for the RTMA database.
    """
    try:
        cursor.close()
        connection.close()
        my_validations_logger.info(
            "RTMA database connection and cursor closed successfully."
        )
    except Exception as e:
        my_validations_logger.error(f"Failed to close RTMA connection or cursor: {e}")


def main():
    """
    Main function to connect to the RTMA database, fetch records, and close the connection.
    Runs the specific scenario for table 'aetna_hourly' and date '2022-03-10'.
    """
    rtma_connection = connect_to_rtma_db()
    if rtma_connection:
        rtma_cursor = get_rtma_cursor(rtma_connection)
        if rtma_cursor:
            table = "aetna_hourly"
            date = "2022-03-10"
            records = fetch_rtma_records(rtma_cursor, table, date)
            print(records)
            rtma_connection.commit()
            close_rtma_connection(rtma_connection, rtma_cursor)


if __name__ == "__main__":
    main()
