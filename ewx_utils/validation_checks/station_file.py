#import sys
import logging
from datetime import datetime, timedelta
from ewx_utils.db_files.dbconnection import connect_to_mawndb
from ewx_utils.db_files.dbconnection  import connect_to_mawndbqc
from ewx_utils.db_files.dbconnection  import mawndb_cursor_connection
from ewx_utils.db_files.dbconnection  import mawnqc_cursor_connection

# def station(station_list(list -actual stations or None), all_stations, None) -> list:
# The function below

logging.basicConfig(filename= 'log_file.log', level = logging.DEBUG,
                    format = '%(asctime)s, %(levelname)s, %(message)s')
logging.debug('This is a debug message')
logging.info('This is an info message')
logging.warning('This is a warning message')
logging.error('This is an error message')
logging.critical('This is a critical message')

mawndb_connection = connect_to_mawndb()
mawndb_cursor = mawndb_cursor_connection(mawndb_connection)
mawndbqc_connection = connect_to_mawndbqc()
mawndbqc_cursor = mawnqc_cursor_connection(mawndbqc_connection)
# creating a sql query to obtain a list of mawndb station names
select_mawndb_stations = """ SELECT table_name FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name like '%hourly'
                    ORDER BY table_name ASC """
                    
select_mawndbqc_stations = """ SELECT table_name FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name like '%hourly'
                    ORDER BY table_name ASC """
# Using the cursor to execute the selecti_stations sql query above
mawndb_cursor.execute(select_mawndb_stations)
mawndb_stations = mawndb_cursor.fetchall()
#print(mawndb_stations)
#print(type(mawndb_stations))
mawndbqc_cursor.execute(select_mawndbqc_stations)
mawndbqc_stations = mawndbqc_cursor.fetchall()
#print(f"mawndbqc_stations: {mawndbqc_stations}")
#print(type(mawndbqc_stations))
mawndb_connection.commit()
mawndbqc_connection.commit()

mawndb_cursor.close()
mawndbqc_cursor.close()
mawndb_connection.close()
mawndbqc_connection.close()

mawn_stns = [dict(row) for row in mawndb_stations]

qc_stns = [dict(row) for row in mawndbqc_stations]
print(mawn_stns)

mawndb_station_list = [sub ['table_name'] for sub in mawn_stns ]
print(mawndb_station_list)
#print(len(mawndb_station_list))

mawndbqc_station_list =  [ sub['table_name'] for sub in qc_stns ]
#print(mawndbqc_station_list)
#print(len(mawndbqc_station_list))

stations_list = []

for stnqc in mawndbqc_station_list:
    if stnqc in mawndb_station_list:
        stations_list.append(stnqc)
logging.debug(f"stations_list: {stations_list}")
#print(len(stations_list))

stations_not_in_qc = []
for stndb in mawndb_station_list:
    if stndb not in mawndbqc_station_list:
        stations_not_in_qc.append(stndb)
logging.debug(f"stations_not_in_qc: {stations_not_in_qc}") 
    
"""
def mawndb_stations_list(specific_station, all_stations):
    try:
        if specific_station is None and all_stations is not None:

        if all_stations is None:
            all_stations = 'aetna_hourly'
def mawndb_stations(stations_list, all_stations):
    stations_list = []
    all_stations = []

"""    


