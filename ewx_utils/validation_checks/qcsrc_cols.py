import sys

sys.path.append("c:/Users/mwangija/data_file/ewx_utils/ewx_utils")
import psycopg2
import psycopg2.extras
import datetime as datetime
import logging
from db_files.dbconnection import connect_to_mawndb
from db_files.dbconnection import connect_to_mawndbqc
from db_files.dbconnection import mawndb_cursor_connection
from db_files.dbconnection import mawnqc_cursor_connection
from validation_logsconfig import validation_logger

my_validation_logger = validation_logger()

mawndb_connection = connect_to_mawndb()
mawndb_cursor = mawndb_cursor_connection(mawndb_connection)
mawndbqc_connection = connect_to_mawndbqc()
mawndbqc_cursor = mawnqc_cursor_connection(mawndbqc_connection)

select_qc_columns = """ SELECT column_name FROM information_schema.columns WHERE table_name = 'aetna_hourly' """
mawndbqc_cursor.execute(select_qc_columns)
qc_columns = mawndbqc_cursor.fetchall()
# print(f"qc_columns: {qc_columns}")

# qc_column_list = [item for sublist in qc_columns for item in sublist]
# print(qc_column_list)
# print(len(qc_column_list))

# Converting RealDictRow into a normal python dictionary
qc_col_dict = [dict(row) for row in qc_columns]
# print(f"(qc_col_list: {qc_col_dict})")

# Extracting the values from key, value items that have a common key column_name
qc_col_list = [sub["column_name"] for sub in qc_col_dict]
print(f"(qc_col_list: {qc_col_list})")
print(len(qc_col_list))

mawndbqc_cursor.execute(
    "SELECT column_name FROM information_schema.columns WHERE table_name = 'aetna_hourly' AND column_name LIKE '%src'"
)
src_columns = mawndbqc_cursor.fetchall()
# print(f"src_columns: {src_columns}")

# Using a select sql statement to retrieve data from mawndb_qcl
# mawndbqcl_select = ("SELECT * FROM aetna_hourly WHERE date = '2022-03-10'")
# records = mawndbqc_cursor.fetchall()
# print(records)

# select_src_columns = """ #SELECT column_name FROM information_schema.columns WHERE table_name = 'aetna_hourly AND column_name LIKE %src' """
"""
mawndbqc_cursor.execute(select_src_columns)
src_columns = mawndbqc_cursor.fetchall()
print(f"src_columns: {src_columns}")
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

# Commit the transaction for mawndb
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
