import sys
sys.path.append('c:/Users/mwangija/data_file/ewx_utils/ewx_utils')
from db_files.dbconnection import connect_to_rtma
from db_files.dbconnection import rtma_cursor_connection
import datetime

rtma_connection = connect_to_rtma()
rtma_cursor = rtma_cursor_connection(rtma_connection)

rtma_select = ("SELECT* FROM aetna_hourly WHERE date = '2022-03-10'")

rtma_cursor.execute(rtma_select)

rtma_records = rtma_cursor.fetchmany(24)
print(rtma_records)

rtma_connection.commit()

rtma_cursor.close()
rtma_connection.close()

