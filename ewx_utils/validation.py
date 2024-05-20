import logging
from db_files.dbconnection import connect_to_mawndb
from db_files.dbconnection import connect_to_mawndbqc
from db_files.dbconnection import mawndb_cursor_connection
from db_files.dbconnection import mawnqc_cursor_connection
from validation_checks.mawndbsrc import mawndb_addingsrcto_cols


mawndb_connection = connect_to_mawndb()
print(mawndb_connection)
mawndb_cursor = mawndb_cursor_connection(mawndb_connection)
print(mawndb_cursor)

 #Using a select sql statement to retrieve data from mawndb 
mawndb_select = ("SELECT * FROM aetna_hourly WHERE date = '2022-03-10'")
logging.INFO(mawndb_select)
mawndb_cursor.execute(mawndb_select)
records = mawndb_cursor.fetchmany(2)



