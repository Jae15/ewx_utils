import psycopg2
import psycopg2.extras
import logging
from psycopg2 import OperationalError
from db_files.configfile import (
    config_mawndb,
    config_mawndbqc,
    config_mawndbrtma,
    config_qctest,
)
from db_files.dbfiles_logs_config import dbfiles_logger

# Initialize custom logger
my_dbfiles_logger = dbfiles_logger()


def connect_to_mawndb():
    """
    Establishes a connection to the mawndb database.

    Returns:
        psycopg2.extensions.connection: Connection object for the mawndb database.
    """
    db_info = config_mawndb()
    mawndb_connection = None  # initialize connection as None

    try:
        mawndb_connection = psycopg2.connect(**db_info)
        my_dbfiles_logger.info("Successfully connected to mawndb")
        mawndb_connection.autocommit = False

    except OperationalError as e:
        my_dbfiles_logger.error(f"Error Connecting to Mawndb Database: {e}")

    return mawndb_connection


def connect_to_mawndbqc():
    """
    Establishes a connection to the mawndb_qc database.

    Returns:
        psycopg2.extensions.connection: Connection object for the mawndb_qc database.
    """
    db_info2 = config_mawndbqc()
    mawndbqc_connection = None  # Initialize connection as None

    try:
        mawndbqc_connection = psycopg2.connect(**db_info2)
        my_dbfiles_logger.info("Successfully connected to mawndb_qc")
        mawndbqc_connection.autocommit = False

    except OperationalError as e:
        my_dbfiles_logger.error("Error connecting to database", exc_info=True)

    return mawndbqc_connection


def connect_to_rtma():
    """
    Establishes a connection to the rtma database.

    Returns:
        psycopg2.extensions.connection: Connection object for the rtma database.
    """
    db_info3 = config_mawndbrtma()
    rtma_connection = None

    try:
        rtma_connection = psycopg2.connect(**db_info3)
        my_dbfiles_logger.info("Successfully connected to RTMA Database")
        rtma_connection.autocommit = False

    except OperationalError as e:
        my_dbfiles_logger.error(f"Error Connecting to RTMA Database: {e}")

    return rtma_connection


def connect_to_qctest():
    """
    Establishes a connection to the qctest database.

    Returns:
        psycopg2.extensions.connection: Connection object for the qctest database.
    """
    db_info4 = config_qctest()
    qctest_connection = None

    try:
        qctest_connection = psycopg2.connect(**db_info4)
        my_dbfiles_logger.info("Successfully connected to QCTEST Database")
        qctest_connection.autocommit = False

    except OperationalError as e:
        my_dbfiles_logger.error(f"Error Connecting to QCTEST Database: {e}")

    return qctest_connection


def mawndb_cursor_connection(mawndb_connection):
    """
    Establishes a cursor connection for the mawndb database.

    Args:
        mawndb_connection (psycopg2.extensions.connection): Connection object for the mawndb database.

    Returns:
        psycopg2.extras.RealDictCursor: Cursor object for the mawndb database.
    """
    try:
        mawndb_cursor = mawndb_connection.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor
        )
        my_dbfiles_logger.info("Mawndb cursor connection successfully established")

    except (Exception, psycopg2.DatabaseError) as MawnCursorError:
        my_dbfiles_logger.error(
            "Error establishing mawndb cursor connection: %s", MawnCursorError
        )
        raise

    return mawndb_cursor


def mawnqc_cursor_connection(mawndbqc_connection):
    """
    Establishes a cursor connection for the mawndb_qc database.

    Args:
        mawndbqc_connection (psycopg2.extensions.connection): Connection object for the mawndb_qc database.

    Returns:
        psycopg2.extras.RealDictCursor: Cursor object for the mawndb_qc database.
    """
    try:
        mawnqc_cursor = mawndbqc_connection.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor
        )
        my_dbfiles_logger.info("Mawnqcl cursor connection successfully established")

    except (Exception, psycopg2.DatabaseError) as MawnqcCursorError:
        my_dbfiles_logger.error(
            "Error establishing mawnqc cursor connection: %s", MawnqcCursorError
        )
        raise

    return mawnqc_cursor


def rtma_cursor_connection(rtma_connection):
    """
    Establishes a cursor connection for the rtma database.

    Args:
        rtma_connection (psycopg2.extensions.connection): Connection object for the rtma database.

    Returns:
        psycopg2.extras.RealDictCursor: Cursor object for the rtma database.
    """
    try:
        rtma_cursor = rtma_connection.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor
        )
        my_dbfiles_logger.info("RTMA cursor connection successfully established")

    except (Exception, psycopg2.DatabaseError) as RtmaCursorError:
        my_dbfiles_logger.error(
            "Error establishing rtma cursor connection: %s", RtmaCursorError
        )
        raise

    return rtma_cursor


def qctest_cursor_connection(qctest_connection):
    """
    Establishes a cursor connection for the qctest database.

    Args:
        qctest_connection (psycopg2.extensions.connection): Connection object for the qctest database.

    Returns:
        psycopg2.extras.RealDictCursor: Cursor object for the qctest database.
    """
    try:
        qctest_cursor = qctest_connection.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor
        )
        my_dbfiles_logger.info("QCTEST cursor connection successfully established")

    except (Exception, psycopg2.DatabaseError) as QctestCursorError:
        my_dbfiles_logger.error(
            "Error establishing qctest cursor connection: %s", QctestCursorError
        )
        raise

    return qctest_cursor
