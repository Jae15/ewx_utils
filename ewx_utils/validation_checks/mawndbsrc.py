#!/usr/bin/python3
import psycopg2
import sys
import pprint
import psycopg2.extras
import datetime as datetime
from dateutil.rrule import *
from dateutil.parser import *
from datetime import *
sys.displayhook = pprint.pprint
import dateutil
import datetime
from datetime import timezone
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta
from dateutil.rrule import rrule, MONTHLY
import logging
logging.debug('This is a debug message')
logging.info('This is an info message')
logging.warning('This is a warning message')
logging.error('This is an error message')
logging.critical('This is a critical message')
from ewx_utils.validation_checks import relh_vars
from ewx_utils.validation_checks import pcpn_vars
from ewx_utils.validation_checks import rpet_vars
from ewx_utils.validation_checks import temp_vars
from ewx_utils.validation_checks import wspd_vars
from ewx_utils.validation_checks import wdir_vars
from ewx_utils.validation_checks import leafwt_vars
from ewx_utils.db_files import connect_to_mawndb
from ewx_utils.db_files import mawndb_cursor_connection
from ewx_utils.mawndb_classes.humidity import humidity
from ewx_utils.mawndb_classes.windspeed import windspeed
from ewx_utils.mawndb_classes.leafwetness import leafwetness
from ewx_utils.mawndb_classes.temperature import temperature
from ewx_utils.mawndb_classes.precipitation import precipitation
from ewx_utils.mawndb_classes.winddirection import winddirection
from ewx_utils.mawndb_classes.evapotranspiration import evapotranspiration


mawndb_connection = connect_to_mawndb()
print(mawndb_connection)
mawndb_cursor = mawndb_cursor_connection(mawndb_connection)
print(mawndb_cursor)

 #Using a select sql statement to retrieve data from mawndb 
mawndb_select = ("SELECT * FROM aetna_hourly WHERE date = '2022-03-10'")
mawndb_cursor.execute(mawndb_select)
records = mawndb_cursor.fetchmany(2)

#print(records)

#Commit the transaction for mawndb
mawndb_connection.commit()


"""Defining the check value function below that takes in the variable key as a string, the value as a float and the date as datetime"""

def check_value(k: str, v: float, d: datetime.datetime): 
    if k in relh_vars:
        rh = humidity(v, 'PCT', d) 
        if rh.IsInRange() and rh.IsValid():
            return True
        else:
            return False
        
    if k in pcpn_vars:
        pn = precipitation(v, 'hourly', 'MM', d)
        if pn.IsValid():
            return True
        else:
            return False
        
    if k in rpet_vars:
        rp = evapotranspiration(v, 'hourly', 'MM', d)
        if rp.IsValid():
            return True
        else:
            return False
        
    if k in temp_vars:
        tp = temperature(v, 'C', d)
        if tp.IsValid():
            return True
        else:
            return False
        
    if k in wspd_vars:
        ws = windspeed(v, 'MPS', d)
        if ws.IsValid():
            return True
        else:
            return False
            
    if k in wdir_vars:
        wd = winddirection(v, 'DEGREES', d)
        if wd.IsValid():
            return True
        else:
            return False

    if k in leafwt_vars:
        lw = leafwetness(v, 'hourly', k, d)
        if lw.IsValid() and lw.IsWet():
            return True
        else: 
            return False

    return True

    
record_keys = []
record_vals = []
db_columns : list
db_columns = []
qc_values : list
qc_values = []
clean_records = []

def mawndb_addingsrcto_cols(records):
    for record in records:
        clean_record = mawndb_srccols_torecord(record)
        clean_records.append(clean_record)
    
def mawndb_srccols_torecord(record):
    record_keys = list(record.keys())
    #print(f"record_keys: {record_keys}")
    record_vals = list(record.values())
    #print(f"record_vals: {record_vals}")
    date_value = record.get("date")
    time_value = record.get("time")
    combined_datetime = datetime.datetime.combine(date_value, time_value)
    #print(combined_datetime)
    clean_record: dict
    clean_record = {}
    for key in record_keys:
        clean_record[key] = None
        #If key + src column is found in the list of columns that contains src, then src is added
        clean_record[key + "_src"] = None
        value_check = check_value(key, record[key], combined_datetime)
        if value_check is True:
            clean_record[key] = record[key]
            clean_record[key + "_src"] = "MAWN"
    return clean_record
    
    
def month_abbrv(record):
    for key, value in record.items():
        if key == "date":
            date_value = record.get("date")
            time_value = record.get("time")
            combined_datetime = datetime.datetime.combine(date_value, time_value)
            #print(combined_datetime)
            datestring = combined_datetime.strftime("%b %d, %Y")
            #print(datestring)
            month_abbrv = datestring[0:3]
    return month_abbrv
            
#print(clean_records)
#print(clean_record)
#print(len(clean_records))
                
"""
After obtaining a clean record we use the above to insert it into mawndbqc
"""

mawndb_connection.close()
mawndb_cursor.close()

