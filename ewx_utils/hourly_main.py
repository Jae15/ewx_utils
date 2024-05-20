#!/usr/bin/python3
import psycopg2
import os, sys, time, shutil, getopt
import datetime as datetime
import pprint
import argparse
import logging
from datetime import timezone
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta
from datetime import datetime
from psycopg2 import OperationalError
from db_files.dbconnection import connect_to_mawndb
from db_files.dbconnection import connect_to_mawndbqc
from db_files.dbconnection import mawndb_cursor_connection
from db_files.dbconnection import mawnqc_cursor_connection
from validation_checks.timeloop import generate_list_of_hours
#from validation_checks.stations_file import stations_list

logging.basicConfig(filename = 'dbconnection', level = logging.DEBUG,
                    format = '%(asctime)s, %(levelname)s, %(message)s')

def main():
    parser = argparse.ArgumentParser(
                      prog = 'hourly_main', 
                      description = 'Checks data from hourly_main in mawndb_qc and adds estimates from fivemin data or RTMA as needed',
                      epilog = 'Check missing data python scripts for help'
                      )
    #parser.add_argument('start_date', help = 'start_date in YYYY-MM-DD', type= str)
    #parser.add_argument('end_date', help = 'end date in YYYY-MM-DD', type= str)
    parser.add_argument('-b', '--begin', type=str, help='(time is not accepted/used) date on which to begin - this script uses this date the date represented, not the mawndate (e.g., to start on 1/1/2023, enter 1/1/2023 (mawndate would be 1/2/2023).  If no begin date is entered, it is calculated backward 7-days from today using timedelta where delta = 7days') 
    parser.add_argument('-e', '--end', type=str, help='(time is not accepted/used) date on which to end - this script uses this date the date represented, not the mawndate (e.g., to end with data that represents 1/5/2023, enter 1/5/2023 (mawndate would be 1/6/2023).  If no end date is entered, default is today.')
    
    parser.add_argument('-f', '--forcedelete', action = 'store_true', help = "use 'blast_old_records', forcing delete of old records(use when we know we have an issue in old data")
    parser.add_argument('-c', '--clearoverride', action = 'store_true', help = "set override_daily 'active' in this time period to false - will not use override data again until reactivated in override_daily table")
    
    group = parser.add_mutually_exclusive_group(required = True)
    group.add_argument('-x', '--execute', action = 'store_true', help = 'execute sql and change data in qc database')
    group.add_argument('-d','--dryrun', action='store_true', help = 'do not execute sql - just write to stdout')
    parser.add_argument('-l', '--log', action = 'store_true', help ='Create a logfile for this run')

    group = parser.add_mutually_exclusive_group()
    
    group.add_argument('-s', '--stations', nargs = '*', type = str, help = 'run this for a specific station(use station_name, not station_id). to run for multiple specific stations, list all after the flag')
    group.add_argument('-a', '--all', action = 'store_true', default = False, help = 'use this flag for all stations')
    
    parser.add_argument('-q', '--qcwrite', type = str, default = 'mawndb_qc:local', choices =['mawndb_qc:local', 'mawndb_qc:dbh11'],
                        help = 'Use this flag to modify data in a specific database (note that if --writefile is selected, the commands will not actually execute/modify data, but this DB will serve as the source)')
        
    parser.add_argument("--mawn", type = str, choices = ['mawndb:dbh11'], default = 'mawndb:dbh11',
                        help = 'Use this flag to read mawndb data from a specific database')
    
    parser.add_argument("--rtma", type = str, choices =['rtma:dbh11'], default = 'rtma:dbh11',
                        help = 'Use this flag to read rtma data from a specific database')
    
    args = parser.parse_args()
    
    print(args)
    
    print(time_defaults(args.begin, args.end))
    
    
    #return
    #print('begin date:',args.begin_date)
    #print('end date:',args.end_date)
    #print(datetime.datetime.strptime(args.start_date, "%Y-%m-%d"))
 
    #datetime_list = generate_list_of_hours(args.start_date, args.end_date)
    #print(datetime_list)

    try: 
        
        mawndb_connection = connect_to_mawndb()
        mawndb_cursor = mawndb_cursor_connection(mawndb_connection)

        # Using a select sql statement to retrieve data from mawndb 
        mawndb_select = ("SELECT * FROM aetna_hourly WHERE date = '2022-03-10'")
        mawndb_cursor.execute(mawndb_select)
        #mawndb_select = (""" SELECT * from aetna_hourly where ("date" >= '2023-10-10' and "date" <= '2023-10-14' ) order by 'date', 'time' """)
        #Fetch the records
        records = mawndb_cursor.fetchmany(2)
        print(records)

        #Commit the transaction for mawndb
        mawndb_connection.commit()
        
        
        record_keys = []
        record_keys: list
        record_vals = []
        db_columns = []
        qc_values = []
        

        for record in records:
            record_keys = list(record.keys())
            record_vals = list(record.values())
            clean_record = {}

                
            db_columns = ', '.join(list(record.keys()))
            #print(f"db_columns: {db_columns}")
            qc_values = ', '.join(['%s'] * len(list(record.keys())))
            #print(f"qc_values: {qc_values}")
            
            mawndbqc_connection = connect_to_mawndbqc()
            mawndbqc_cursor = mawnqc_cursor_connection(mawndbqc_connection)
            
            #Creating the SQL query with placeholders for table name, columns, and value
            query = """ INSERT INTO %s(%s) VALUES(%s); """ % ('aetna_hourly', db_columns, qc_values)
            print(mawndbqc_cursor.mogrify(query, record_vals))
            mawndbqc_cursor.execute(query, record_vals)
            
            #Commit the transaction for mawndbqc
            mawndb_connection.commit()

    except OperationalError as e:
        logging.error(f"Error connecting to the database: {e}") # Log error connecting to the database
        #print("Error connecting to the database :/")

    finally:
        if mawndbqc_cursor:
            mawndbqc_cursor.close()
        if mawndbqc_connection:
            mawndbqc_connection.close()
    
        if mawndb_cursor:
            mawndb_cursor.close()
        if mawndb_connection:
            mawndb_connection.close()

        

def time_defaults(user_begin_date: str, user_end_date: str):
    
    """time_defaults
    :param user_begin_date: user input - should be less than user_end_date
    :type user_begin_date: str
    :param user_end_date: user input - should be greater than user_begin_date
    :type user_end_date: str
    :raises ValueError: _description_
    :return: _description_
    :rtype: _type_
    
    """
    try:
        if user_begin_date is None:
            user_begin_date = datetime.now() - timedelta(days=7)

             # calculate date and time
        if user_end_date is None:
            user_end_date = user_begin_date + timedelta(days=7)
            
        begin_date = datetime.strftime(user_begin_date, '%Y-%m-%d')
        end_date = datetime.strftime(user_end_date, '%Y-%m-%d')
        
        if begin_date > end_date:
            raise ValueError("User begin date should come before user end date")
        
        return user_begin_date, user_end_date
    except ValueError as ve:
        logging.error("ValueError ocurred: %s", ve) # Logging the error
        print("ValueError:", ve)
    except Exception as e:
        logging.error("An error has occurred: %s", exc_info = e)
        print("An error has occurred", e)
    finally:
        logging.info("Function execution completed")
    
#group.add_argument('-s', '--stations', nargs = '*', type = str, help = 'run this for a specific station(use station_name, not station_id). to run for multiple specific stations, list all after the flag')
#group.add_argument('-a', '--all', action = 'store_true', default = False, help = 'use this flag for all stations')
# def station(station_list(list -actual stations or None), all_stations, None) -> list:

if __name__ == "__main__":
  main()
      
