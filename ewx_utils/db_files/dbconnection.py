import psycopg2
import psycopg2.extras
import logging
from psycopg2 import OperationalError
#import os, sys, time, shutil, getopt
from .configfile import config_mawndb
from .configfile import config_mawndbqc

""" 
This file contains functions used to connect to databases 
The functions contain connection information as stored in the configfile.

logging.basicConfig(filename= 'log_file.log', level = logging.DEBUG,
                    format = '%(asctime)s, %(levelname)s, %(message)s')
#logging.debug('This is a debug message')
#logging.info('This is an info message')
#logging.warning('This is a warning message')
#logging.error('This is an error message')
#logging.critical('This is a critical message')

"""
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.FOrmatter('%(levelname)s - %(asctime)s - %(message)s')
file_handler = logging.FileHandler('log_file.log')
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.DEBUG)
logger.addHandler(file_handler)

def connect_to_mawndb():
    db_info = config_mawndb()
    mawndb_connection = None # Initializing mawndb_connection as None

    try:
        mawndb_connection = psycopg2.connect(**db_info)
        logging.info("Successfully connected to mawndb") # Log successful connection
        mawndb_connection.autocommit = False # Set autocommit to False

    except OperationalError as e:
        logging.error(f"Error Connecting to Mawndb Database: {e}") # Log error connecting to the database

    return mawndb_connection

def connect_to_mawndbqc():
    db_info2 = config_mawndbqc()
    mawndbqc_connection = None # Initializing connection to None

    try:
        mawndbqc_connection = psycopg2.connect(**db_info2)
        logging.info("Successfully connected to mawndb_qc") # Log successful connection
        mawndbqc_connection.autocommit = False # Set autocommit to False

    except OperationalError as e:
        logging.error(f"Error Connecting to Mawndb_qc Database: {e}") # Log error connecting to the database

    return mawndbqc_connection


# Functions to set up cursors for mawndb and mawndb_qcl connections
def mawndb_cursor_connection(mawndb_connection):
    try:
        mawndb_cursor = mawndb_connection.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
        logging.info("Mawndb cursor connection successfully established") # Log successful cursor connection

    except(Exception, psycopg2.DatabaseError) as MawnCursorError:
        logging.error("Error establishing mawndb cursor connection: %s", MawnCursorError) # Log cursor connection error

    return mawndb_cursor


def mawnqc_cursor_connection(mawndbqc_connection):
    try:
        mawnqc_cursor = mawndbqc_connection.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
        logging.info("Mawnqcl cursor connection successfully established") # Log successful cursor connection

    except(Exception, psycopg2.DatabaseError) as MawnqcCursorError:
        logging.error("Error establishing mawnqc cursor connection: %s", MawnqcCursorError) # Log cursor connection error

    return mawnqc_cursor


