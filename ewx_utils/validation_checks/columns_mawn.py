import sys
import psycopg2
import logging
logging.debug('This is a debug message')
logging.info('This is an info message')
logging.warning('This is a warning message')
logging.error('This is an error message')
logging.critical('This is a critical message')
from psycopg2 import OperationalError
from ewx_utils.db_files.configfile import config_mawndb 
from ewx_utils.db_files.configfile import config_mawndbqc
from ewx_utils.db_files.dbconnection import connect_to_mawndb
from ewx_utils.db_files.dbconnection import mawndb_cursor_connection
from validation_logsconfig import validations_logger

validation_logger = validations_logger()
validation_logger.error("Remember to log errors using my_logger")
#logger = logging.getLogger(__name__)


def get_station_column_key(station_name, db_cursor):
    list_of_db_columns = []
    list_of_db_tables = []
    
    select_list_of_tables = """
    select table_name
    from information_schema.tables
    where table_schema = 'public'
    and table_name like '%hourly'
    order by table_name asc;
    """
    
    select_list_of_columns = """
    SELECT column_name 
    FROM information_schema.columns
    WHERE table_schema = 'public' 
    AND table_name = %s 
    ORDER BY column_name ASC;
    """
    
    mawndb_connection = connect_to_mawndb()
    print(mawndb_connection)
    
    mawndb_cursor = mawndb_cursor_connection(mawndb_connection)
    
    mawndb_cursor.execute(list_of_db_columns)
    
    mawndb_cursor.execute(select_list_of_columns, (station_name,))
    
    db_column_list = mawndb_cursor.fetchall()
    
    for db_column in db_column_list:
        list_of_db_columns.append(db_column[0])

    print(list_of_db_columns)
    
    get_station_column_key('aetna_hourly', mawndb_cursor)
    
    mawndb_connection.close()
    mawndb_cursor.close()



