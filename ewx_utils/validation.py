import logging
from db_files.dbconnection import connect_to_mawndb
from db_files.dbconnection import connect_to_mawndbqc
from db_files.dbconnection import mawndb_cursor_connection
from db_files.dbconnection import mawnqc_cursor_connection
#from validation_checks.mawndbsrc import mawndb_addingsrcto_cols
#from validation_checks.mawndbbsrc import check_value
#from validation_checks.mawndbsrc import mawndb_srccols_torecord

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)s - %(asctime)s - %(message)s')
file_handler = logging.FileHandler('validation.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

 #Using a select sql statement to retrieve data from mawndb 
mawndb_select = ("SELECT * FROM aetna_hourly WHERE date = '2022-03-10'")
logging.INFO(mawndb_select)
mawndb_cursor_connection.execute(mawndb_select)
records = mawndb_cursor_connection.fetchmany(2)

mawndbqc_connection = connect_to_mawndbqc()
mawndbqc_cursor = mawnqc_cursor_connection(mawndbqc_connection)

#Creating the SQL query with placeholders for table name, columns, and value
query = """ INSERT INTO %s(%s) VALUES(%s); """ % ('aetna_hourly', db_columns, qc_values)
print(mawndbqc_cursor.mogrify(query, record_vals))
mawndbqc_cursor.execute(query, record_vals)

connect_to_mawndb.close()
connect_to_mawndbqc.close()



