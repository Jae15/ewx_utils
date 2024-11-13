from typing import List, Dict, Any
import ewx_utils.ewx_config as ewx_config 
from logs.ewx_utils_logs_config import ewx_utils_logger

# Initialize the logger
my_validation_logger = ewx_utils_logger(path = ewx_config.ewx_log_file)

# Import database connection functions
from ewx_utils.db_files.dbs_connections import (
    connect_to_mawn_dbh11,
    connect_to_mawnqc_dbh11,
    connect_to_rtma_dbh11,
    mawn_dbh11_cursor_connection,
    mawnqc_dbh11_cursor_connection,
    rtma_dbh11_cursor_connection,
)

def fetch_station_list(cursor, query: str) -> List[Dict[str, Any]]:
    """Fetches the list of stations from the database."""
    cursor.execute(query)
    return cursor.fetchall()

def get_all_stations(cursor, query: str) -> List[str]:
    """Fetches all stations from the mawndb database based on the provided query."""
    stations = fetch_station_list(cursor, query)
    return [dict(row)["table_name"] for row in stations]  

def get_db_connections():
    """Establish connections to the databases and return connection and cursor objects."""
    mawndb_connection = connect_to_mawn_dbh11()
    mawndb_cursor = mawn_dbh11_cursor_connection(mawndb_connection)
    mawndbqc_connection = connect_to_mawnqc_dbh11()
    mawndbqc_cursor = mawnqc_dbh11_cursor_connection(mawndbqc_connection)
    rtma_connection = connect_to_rtma_dbh11()
    rtma_cursor = rtma_dbh11_cursor_connection(rtma_connection)
    return (
        mawndb_connection,
        mawndb_cursor,
        mawndbqc_connection,
        mawndbqc_cursor,
        rtma_connection,
        rtma_cursor,
    )

def close_db_connections(connections: List[Any], cursors: List[Any]):
    """Close the database connections and cursors."""
    for cursor in cursors:
        cursor.close()
    for connection in connections:
        connection.close()

def db_stations(mawndb_station_list, mawndbqc_station_list) -> List[str]:
    """Get the list of stations that are in both mawndb and mawndbqc."""
    return [stnqc for stnqc in mawndb_station_list if stnqc in mawndbqc_station_list]

def main():
    try:
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

        # Fetch all stations from mawndb
        all_mawndb_stations = get_all_stations(mawndb_cursor)
        my_validation_logger.debug(f"All Mawndb stations: {all_mawndb_stations}")

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
        my_validation_logger.debug(f"RTMA stations: {rtma_station_list}")

        stations_list = db_stations(mawndb_station_list, mawndbqc_station_list)
        my_validation_logger.debug(f"DB Stations: {stations_list}")

        def extra_stations() -> List[str]:
            """Get the list of stations that are in mawndb but not in mawndbqc."""
            stations_not_in_qc = [
                stndb for stndb in mawndb_station_list if stndb not in mawndbqc_station_list
            ]
            my_validation_logger.debug(f"Stations not in QC: {stations_not_in_qc}")

    except Exception as e:
        my_validation_logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()