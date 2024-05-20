import sys
import psycopg2
import psycopg2.extras
import datetime as datetime
import logging
logging.debug('This is a debug message')
logging.info('This is an info message')
logging.warning('This is a warning message')
logging.error('This is an error message')
logging.critical('This is a critical message')
from ewx_utils.db_files.dbconnection import connect_to_mawndb
from ewx_utils.db_files.dbconnection import connect_to_mawndbqc
from ewx_utils.db_files.dbconnection import mawndb_cursor_connection
from ewx_utils.db_files.dbconnection import mawnqc_cursor_connection

mawndb_connection = connect_to_mawndb()
mawndb_cursor = mawndb_cursor_connection(mawndb_connection)
mawndbqc_connection = connect_to_mawndbqc()
mawndbqc_cursor = mawnqc_cursor_connection(mawndbqc_connection)

mawndbqc_cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'aetna_hourly' ")
qc_columns = mawndbqc_cursor.fetchall()
print(f"qc_columns: {qc_columns}")

mawndbqc_cursor().execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'aetna_hourly' AND column_name LIKE '%src'")
src_columns = mawndbqc_cursor.fetchall()
#print(src_columns)
#Using a select sql statement to retrieve data from mawndb_qcl 
#mawndbqcl_select = ("SELECT * FROM aetna_hourly WHERE date = '2022-03-10'")
#records = mawndbqc_cursor.fetchall()
#print(records)
    
"""
for record in records:
    for col in record:
        col_name = col
        if not col_name.endswith('_src'):
            #checking if columnn is not NULL
            if record[col_name] is not None:
                #Replace corresponding src column with 'MAWN'
                record[col_name + '_src'] = 'MAWN' 

# Iterate through the columns and perform the updates in bulk
for col in qc_columns:
    col_name = col[0]
    if not col_name.endswith('_src') and col_name in src_columns:
        # Generate and execute the SQL update statement for the non-src column and its corresponding src column
        update_query = f"UPDATE aetna_hourly {col_name}_src = 'MAWN' WHERE {col_name} IS NOT NULL;"
        mawndbqc_cursor.execute(update_query)
"""
        
#Commit the transaction for mawndb
mawndbqc_connection.commit()

"""
# Generate and execute the update queries
for column in src_columns:
    column_name = column[0]
    update_query = f"UPDATE aetna_hourly SET {column_name} = 'MAWN' WHERE {column_name} is NULL;"
    mawndbqc_cursor.execute(update_query)
"""

# Close the cursor and connection for mawndbqc
mawndbqc_cursor.close()
mawndbqc_connection.close()






