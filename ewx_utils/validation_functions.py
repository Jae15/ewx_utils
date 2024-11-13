from ewx_utils.db_files.dbs_connections import connect_to_mawndb
from ewx_utils.db_files.dbs_connections import connect_to_rtma
from ewx_utils.db_files.dbs_connections import connect_to_mawndbqc
from ewx_utils.db_files.dbs_connections import connect_to_qctest
from ewx_utils.db_files.dbs_connections import mawndb_cursor_connection
from ewx_utils.db_files.dbs_connections import rtma_cursor_connection
from ewx_utils.db_files.dbs_connections import mawnqc_cursor_connection
from ewx_utils.db_files.dbs_connections import qctest_cursor_connection
import logging
from validation_checks.validation_logsconfig import validations_logger

my_validation_logger = validations_logger()
my_validation_logger.error("Remember to log errors using my_logger")
#logger = logging.getLogger(__name__)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(levelnames)s - %(message)s")
file_handler = logging.FileHandler("log_file")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

def record_returned():
    if record_returned is not None:
        return True
    else:
        return False

    """
    boolean function that checks if the select statement returns a record or not
    It returns true if there is a record and false otherwise.
    If record is returned ie function returns True, send it to validation check
    Generic function that would work with a record from any database
    The select statement returns a record or records. 
    So essentially, we are checking if the record was returned successfully or not.
    Another function: record not in mawndb or rather if its null in mawndb + not null in mawndb_rtma + within range + valid 
    in mawndb_rtma, then retrieve it

    """
qctest_connection = connect_to_qctest()
qctest_cursor = qctest_cursor_connection(qctest_connection)

rtma_connection = connect_to_rtma()
rtma_cursor = rtma_cursor_connection(rtma_connection)
def retrieve_rtma_records(station_name, db_column, date_entry):
    rtma_select = (f"SELECT {db_column} FROM {station_name}_hourly WHERE"
                   f"date = {date_entry: %Y-%m-%d} AND time = {date_entry: '%H:00:00'}")
    try:
        rtma_cursor.execute(rtma_select)
        rtma_records = rtma_cursor.fetchone()
        if rtma_records:
            if rtma_records != None:
                return rtma_records[0]
    except:
        print('exception')

    return None 

