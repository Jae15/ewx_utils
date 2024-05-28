import logging
from db_files.dbconnection import connect_to_mawndb
from db_files.dbconnection import connect_to_mawndbqc
from db_files.dbconnection import connect_to_qctest
from db_files.dbconnection import connect_to_rtma
from db_files.dbconnection import mawndb_cursor_connection
from db_files.dbconnection import mawnqc_cursor_connection
from db_files.dbconnection import rtma_cursor_connection
from db_files.dbconnection import qctest_cursor_connection
#from validation_checks.mawndbsrc import mawndb_addingsrcto_cols
#from validation_checks.mawndbbsrc import check_value
#from validation_checks.mawndbsrc import mawndb_srccols_torecord

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(levelnames)s - %(message)s")
file_handler = logging.FileHandler("log_file.log")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


def validation(db_station, date_entry):
    #date_entry = (user_begin_date, user_end_date)
    mawndb_connection = connect_to_mawndb()
    mawndb_cursor = mawndb_cursor_connection(mawndb_connection)

    # Using a select sql statement to retrieve data from mawndb 
    mawndb_select = (f"SELECT * FROM {db_station} WHERE date = %s", (date_entry))
    

    #mawndb_select = (f" SELECT * FROM %s_hourly WHERE date = '%s'")
    mawndb_cursor.execute(mawndb_select)
    #mawndb_select = (""" SELECT * from aetna_hourly where ("date" >= '2023-10-10' and "date" <= '2023-10-14' ) order by 'date', 'time' """)
    #Fetch the records
    records = mawndb_cursor.fetchall()
    print(records)

    rtma_select = ()
    # Commit the transaction for mawndb
    mawndb_connection.commit()

    mawndb_cursor.close()
    mawndb_connection.close()
    
"""
if __name__ == "__main__":
    main()
"""
