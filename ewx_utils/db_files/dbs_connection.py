import os
import sys
import argparse
from argparse import Namespace
import psycopg2
from psycopg2 import OperationalError, extras
from typing import Any
from dotenv import load_dotenv
load_dotenv()
ewx_base_path = os.getenv("EWX_BASE_PATH")
sys.path.append(ewx_base_path)
from ewx_utils.ewx_config import ewx_log_file
from ewx_utils.logs.ewx_utils_logs_config import ewx_utils_logger
from typing import List, Dict, Any, Tuple
from ewx_utils.db_files.dbs_configfile import get_db_config

# Initialize custom logger
my_dbfiles_logger = ewx_utils_logger(log_path=ewx_log_file)

def connect_to_db(db_info: dict, autocommit: bool = False) -> psycopg2.extensions.connection:
    """
    Establishes a connection to the specified database using the provided configuration dictionary.
    
    Parameters:
        db_info(dict): Dictionary containing database configuration details
        autocommit (bool, optional): Set autocommit mode for the connection.
        Defaults to False

    Returns:
        psycopg2.extensions.connection: Connection object for the database

    Raises:
        ValueError: If db_info is empty or None
        OperationalError: IF connection to database fails
        KeyError: If required database configuration is missing
    """
    if not db_info:
        my_dbfiles_logger.error("Databse configuration dictionary cannot be empty or None")
        raise ValueError("Database configuration dictionary cannot be empty or None")
    
    try:
        connection = psycopg2.connect(**db_info)
        connection.autocommit = autocommit
        my_dbfiles_logger.info(f"Successfully connected to {db_info.get('dbname', 'unknown')} "
                              f"on {db_info.get('host', 'unknown')}")
        return connection
    
    except OperationalError as e:
        my_dbfiles_logger.error(f"Database connection error for {db_info.get('dbname','unkown')} "
                                f"on {db_info.get('host', 'unknown')}: {str(e)}")
        raise
    except KeyError as e:
        my_dbfiles_logger.error(f"Configuration error: {str(e)}")
        raise
    except Exception as e:
        my_dbfiles_logger.error(f"Unexpected error while connecting to database: {str(e)}")
        raise

def get_mawn_cursor(connection: psycopg2.extensions.connection,
                  db_name: str,
                  cursor_factory: Any = extras.RealDictCursor) -> psycopg2.extensions.cursor:
    """
    Establishes a cursor connection for a given database connection.

    Parameters:
        connection (psycopg2.extensions.connection): Database connection object
        db_name (str): Name of the database for logging
        cursor_factory (Any, Optional): Cursor factoey to use. Defaults to RealDictCursor
    
    Returns:
        psycopg2.extensions.cursor: Cursor object for database

    Returns:
        ValueError: If connection is None or db_name is empty
        psycopg2.DatabaseError: If cursor creation fails
        Exception: For any other unexpected errors
    """
    if not connection:
        my_dbfiles_logger.error("Database connection cannot be None")
        raise ValueError("Database connection cannot be None")
    
    if not db_name:
        my_dbfiles_logger.error("Database name cannot be empty")
        raise ValueError("Database name cannot be empty")
    
    try:
        mawn_cursor = connection.cursor(cursor_factory=cursor_factory)
        my_dbfiles_logger.info(f"{db_name} cursor connection successfully established")
        return mawn_cursor
    
    except psycopg2.DatabaseError as error:
        my_dbfiles_logger.error(f"Database error establishing {db_name} cursor connection: {str(error)}")
        raise

    except Exception as error:
        my_dbfiles_logger.error(f"Unexpected error establishing {db_name} cursor connection: {str(error)}")
        raise

def get_rtma_cursor(connection: psycopg2.extensions.connection,
                  db_name: str,
                  cursor_factory: Any = extras.RealDictCursor) -> psycopg2.extensions.cursor:
    """
    Establishes a cursor connection for a given database connection.

    Parameters:
        connection (psycopg2.extensions.connection): Database connection object
        db_name (str): Name of the database for logging
        cursor_factory (Any, Optional): Cursor factoey to use. Defaults to RealDictCursor
    
    Returns:
        psycopg2.extensions.cursor: Cursor object for database

    Returns:
        ValueError: If connection is None or db_name is empty
        psycopg2.DatabaseError: If cursor creation fails
        Exception: For any other unexpected errors
    """
    if not connection:
        my_dbfiles_logger.error("Database connection cannot be None")
        raise ValueError("Database connection cannot be None")
    
    if not db_name:
        my_dbfiles_logger.error("Database name cannot be empty")
        raise ValueError("Database name cannot be empty")
    
    try:
        rtma_cursor = connection.cursor(cursor_factory=cursor_factory)
        my_dbfiles_logger.info(f"{db_name} cursor connection successfully established")
        return rtma_cursor
    
    except psycopg2.DatabaseError as error:
        my_dbfiles_logger.error(f"Database error establishing {db_name} cursor connection: {str(error)}")
        raise

    except Exception as error:
        my_dbfiles_logger.error(f"Unexpected error establishing {db_name} cursor connection: {str(error)}")
        raise

def get_qcwrite_cursor(connection: psycopg2.extensions.connection,
                  db_name: str,
                  cursor_factory: Any = extras.RealDictCursor) -> psycopg2.extensions.cursor:
    """
    Establishes a cursor connection for a given database connection.

    Parameters:
        connection (psycopg2.extensions.connection): Database connection object
        db_name (str): Name of the database for logging
        cursor_factory (Any, Optional): Cursor factoey to use. Defaults to RealDictCursor
    
    Returns:
        psycopg2.extensions.cursor: Cursor object for database

    Returns:
        ValueError: If connection is None or db_name is empty
        psycopg2.DatabaseError: If cursor creation fails
        Exception: For any other unexpected errors
    """
    if not connection:
        my_dbfiles_logger.error("Database connection cannot be None")
        raise ValueError("Database connection cannot be None")
    
    if not db_name:
        my_dbfiles_logger.error("Database name cannot be empty")
        raise ValueError("Database name cannot be empty")
    
    try:
        qcwrite_cursor = connection.cursor(cursor_factory=cursor_factory)
        my_dbfiles_logger.info(f"{db_name} cursor connection successfully established")
        return qcwrite_cursor
    
    except psycopg2.DatabaseError as error:
        my_dbfiles_logger.error(f"Database error establishing {db_name} cursor connection: {str(error)}")
        raise

    except Exception as error:
        my_dbfiles_logger.error(f"Unexpected error establishing {db_name} cursor connection: {str(error)}")
        raise
    

def create_db_connections(args: Namespace) -> Dict[str, Any]:
    """
    Create and return necessary database connections based on user-specified arguments.

    Parameters:
    args : Namespace
        An object containing user-specified arguments for database connections.
        Including optional 'read_from' (list of sections) and required 'write_to' (section name).
    
    Returns:
    Dict[str, Any]
        A dictionary containing the created database connections,
        where keys are connection names and values are the corresponding connection objects.
    
    Raises:
    OperationalError:
        If there is an operational error while connecting to the database.
    Exception
        For any unexpected errors that occur during the connection process.
    """

    my_dbfiles_logger.info("Starting database connection process")
    connections = {}

    # Handling optional read connections
    if hasattr(args, 'read_from') and args.read_from:
        my_dbfiles_logger.debug(f"Processing read command from sections: {args.read_from}")
        
        for section in args.read_from:
            try:
                my_dbfiles_logger.info(f"Processing read section: {section}")
                
                # Getting and validating read-from configuration
                db_config = get_db_config(section)
                if not db_config:
                    my_dbfiles_logger.error(f"No configuration found for section: {section}")
                    raise ValueError(f"Missing configuration for {section}")

                # Logging configuration (excluding password)
                safe_config = {k: v for k, v in db_config.items() if k != 'password'}
                my_dbfiles_logger.debug(f"Configuration for {section}: {safe_config}")

                # Creating connection with specific key based on section
                if "mawn" in section:
                    connection_key = "mawn_connection"
                elif "rtma" in section:
                    connection_key = "rtma_connection"
                else:
                    connection_key = f"{section}_connection"
                
                my_dbfiles_logger.debug(f"Attempting connection for {connection_key}")
                connections[connection_key] = connect_to_db(db_config)
                
                # Verifying connection
                if connections[connection_key].closed:
                    raise ConnectionError(f"Connection {connection_key} closed immediately after creation")
                    
                my_dbfiles_logger.info(f"Successfully created {connection_key}")

            except Exception as e:
                my_dbfiles_logger.error(f"Failed to create {section} connection: {str(e)}")
                raise

    # Handle required write-to connection
    try:
        my_dbfiles_logger.info(f"Processing write section: {args.write_to}")
        
        # Getting and validating write-to configuration
        write_config = get_db_config(args.write_to)
        if not write_config:
            my_dbfiles_logger.error(f"No configuration found for write section: {args.write_to}")
            raise ValueError(f"Missing configuration for {args.write_to}")

        # Log write configuration (excluding password)
        safe_write_config = {k: v for k, v in write_config.items() if k != 'password'}
        my_dbfiles_logger.debug(f"Write configuration: {safe_write_config}")

        # Creating write connection
        connections["qcwrite_connection"] = connect_to_db(write_config)
        
        # Verifying write connection
        if connections["qcwrite_connection"].closed:
            raise ConnectionError("Write connection closed immediately after creation")
            
        my_dbfiles_logger.info("Successfully created write connection")

    except Exception as e:
        my_dbfiles_logger.error(f"Failed to create write connection: {str(e)}")
        raise

    my_dbfiles_logger.info("Database connection process completed")
    return connections

