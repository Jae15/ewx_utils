import sys
import logging
from typing import List, Dict, Any
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta

# Append the path for importing custom modules
sys.path.append("c:/Users/mwangija/data_file/ewx_utils/ewx_utils")
# Import database connection functions
from db_files.dbconnection import (
    connect_to_mawndb,
    connect_to_mawndbqc,
    connect_to_rtma,
)
from db_files.dbconnection import (
    mawndb_cursor_connection,
    mawnqc_cursor_connection,
    rtma_cursor_connection,
)
from validation_logsconfig import validations_logger

# Set up validation logger using validations_logger() function from validation_logsconfig.py
logger = validations_logger()


def fetch_station_list(cursor, query: str) -> List[Dict[str, Any]]:
    """
    Fetches the list of stations from the database.

    Args:
        cursor: Database cursor.
        query: SQL query to fetch station names.

    Returns:
        A list of dictionaries containing station names.
    """
    cursor.execute(query)
    return cursor.fetchall()


def get_db_connections():
    """
    Establish connections to the databases and return connection and cursor objects.

    Returns:
        Tuple of connections and cursors for mawndb, mawndbqc, and rtma.
    """
    mawndb_connection = connect_to_mawndb()
    mawndb_cursor = mawndb_cursor_connection(mawndb_connection)
    mawndbqc_connection = connect_to_mawndbqc()
    mawndbqc_cursor = mawnqc_cursor_connection(mawndbqc_connection)
    rtma_connection = connect_to_rtma()
    rtma_cursor = rtma_cursor_connection(rtma_connection)
    return (
        mawndb_connection,
        mawndb_cursor,
        mawndbqc_connection,
        mawndbqc_cursor,
        rtma_connection,
        rtma_cursor,
    )


def close_db_connections(connections: List[Any], cursors: List[Any]):
    """
    Close the database connections and cursors.

    Args:
        connections: List of database connections.
        cursors: List of database cursors.
    """
    for cursor in cursors:
        cursor.close()
    for connection in connections:
        connection.close()


def main():
    try:
        # SQL queries to obtain a list of station names
        select_stations_query = """SELECT table_name FROM information_schema.tables
                                   WHERE table_schema = 'public' AND table_name like '%hourly'
                                   ORDER BY table_name ASC"""

        # Establish database connections and get cursors
        (
            mawndb_connection,
            mawndb_cursor,
            mawndbqc_connection,
            mawndbqc_cursor,
            rtma_connection,
            rtma_cursor,
        ) = get_db_connections()

        # Fetch station lists from the databases
        mawndb_stations = fetch_station_list(mawndb_cursor, select_stations_query)
        mawndbqc_stations = fetch_station_list(mawndbqc_cursor, select_stations_query)
        rtma_stations = fetch_station_list(rtma_cursor, select_stations_query)

        # Commit the transactions
        mawndb_connection.commit()
        mawndbqc_connection.commit()
        rtma_connection.commit()

        # Close database connections and cursors
        close_db_connections(
            [mawndb_connection, mawndbqc_connection, rtma_connection],
            [mawndb_cursor, mawndbqc_cursor, rtma_cursor],
        )

        # Convert RealDictRow from psycopg2.extras into a normal Python dictionary
        mawndb_station_list = [dict(row)["table_name"] for row in mawndb_stations]
        mawndbqc_station_list = [dict(row)["table_name"] for row in mawndbqc_stations]
        rtma_station_list = [dict(row)["table_name"] for row in rtma_stations]

        # Print RTMA station list
        logger.debug(f"RTMA stations: {rtma_station_list}")

        def db_stations() -> List[str]:
            """
            Get the list of stations that are in both mawndb and mawndbqc.

            Returns:
                List of station names.
            """
            return [
                stnqc for stnqc in mawndb_station_list if stnqc in mawndb_station_list
            ]

        stations_list = db_stations()
        logger.debug(f"DB Stations: {stations_list}")

        def extra_stations() -> List[str]:
            """
            Get the list of stations that are in mawndb but not in mawndbqc.

            Returns:
                List of station names.
            """
            stations_not_in_qc = [
                stndb
                for stndb in mawndb_station_list
                if stndb not in mawndbqc_station_list
            ]
            logger.debug(f"Stations not in QC: {stations_not_in_qc}")
            return stations_not_in_qc

        extra_stations_list = extra_stations()
        logger.debug(f"Extra Stations: {extra_stations_list}")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()
