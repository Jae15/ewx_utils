from db_files.dbconnection import connect_to_mawndb
from db_files.dbconnection import connect_to_mawndbqc
from db_files.dbconnection import connect_to_rtma
from db_files.dbconnection import connect_to_qctest
from db_files.dbconnection import mawndb_cursor_connection
from db_files.dbconnection import mawnqc_cursor_connection
from db_files.dbconnection import rtma_cursor_connection
from db_files.dbconnection import qctest_cursor_connection
from validation_checks.mawndbsrc import mawndb_addingsrcto_cols
from validation_checks.mawndbsrc import mawndb_srccols_torecord
from validation_checks.mawndbsrc import clean_records
from validation_checks.mawndbsrc import clean_record

# Creating connections and cursors
mawndb_connection = connect_to_mawndb()
mawndb_cursor = mawndb_cursor_connection(mawndb_connection)

mawnqc_connection = connect_to_mawndbqc
mawnqc_cursor = mawndb_cursor_connection(mawnqc_connection)

rtma_connection = connect_to_rtma()
rtma_cursor = rtma_cursor_connection(rtma_connection)

qctest_connection = connect_to_qctest()
qctest_cursor = qctest_cursor_connection(qctest_connection)

# Using a select sql statement to retrieve data from mawndb 
mawndb_select = ("SELECT * FROM aetna_hourly WHERE date = '2022-03-10'")
mawndb_cursor.execute(mawndb_select)
#mawndb_select = (""" SELECT * from aetna_hourly where ("date" >= '2023-10-10' and "date" <= '2023-10-14' ) order by 'date', 'time' """)
#Fetch the records
mawn_records = mawndb_cursor.fetchmany(2)
print(mawn_records)
#Commit the transaction for mawndb
mawndb_connection.commit()

rtma_select = ("SELECT * FROM aetna_hourly WHERE date = '2023-05-25")
rtma_cursor.execute(rtma_select)

rtma_records = rtma_cursor.fetchmany(2)
print(rtma_records)

rtma_connection.commit()

     
record_keys = []
record_keys: list
record_vals = []
db_columns = []
qc_values = []


for record in mawn_records:
    record_keys = list(record.keys())
    record_vals = list(record.values())
    clean_record = {}

    
db_columns = ', '.join(list(record.keys()))
#print(f"db_columns: {db_columns}")
qc_values = ', '.join(['%s'] * len(list(record.keys())))
#print(f"qc_values: {qc_values}")

# Setting up mawndbqc connection and cursor
mawndbqc_connection = connect_to_mawndbqc()
mawnqc_cursor = mawnqc_cursor_connection(mawndbqc_connection)


# Creating the SQL query with placeholders for table name, columns, and value
query = """ INSERT INTO %s(%s) VALUES(%s); """ % ('aetna_hourly', db_columns, qc_values)
#print(mawnqc_cursor.mogrify(query, record_vals))
#mawnqc_cursor.execute(query, record_vals)

# Commit the transaction for mawndbqc
mawndbqc_connection.commit()

# Setting up qctest connection and cursor
qctest_connection = connect_to_qctest()
qctest_cursor = qctest_cursor_connection(qctest_connection)

# Creating the SQL query with placeholders for table name, columns and value
print(qctest_cursor.mogrify(query, record_vals))
qctest_cursor.execute(query, record_vals)

# Commit the transaction for qctest
qctest_connection.commit()