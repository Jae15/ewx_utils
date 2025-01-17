import os
import sys
from dotenv import load_dotenv

load_dotenv()
ewx_base_path = os.getenv("EWX_BASE_PATH")
sys.path.append(ewx_base_path)
import psycopg2
import psycopg2.extras
from psycopg2 import OperationalError
from ewx_utils.ewx_config import ewx_log_file
from ewx_utils.logs.ewx_utils_logs_config import ewx_utils_logger

my_dbfiles_logger = ewx_utils_logger(log_path=ewx_log_file)

from ewx_utils.db_files.dbs_configfile import (
    config_mawn_dbh11,
    config_mawn_supercell,
    config_mawnqc_dbh11,
    config_mawnqc_supercell,
    config_rtma_dbh11,
    config_rtma_supercell,
    config_mawnqcl,
    config_mawnqc_test,
)


def connect_to_mawn_dbh11():
    """
    Establishes a connection to the mawndb_dbh11 database.

    Returns:
        psycopg2.extensions.connection: Connection object for the mawndb database.
    """
    db_info = config_mawn_dbh11()
    mawn_dbh11_connection = None  # initialize connection as None

    try:
        mawn_dbh11_connection = psycopg2.connect(**db_info)
        my_dbfiles_logger.info("Successfully connected to mawndb")
        mawn_dbh11_connection.autocommit = False

    except OperationalError as e:
        my_dbfiles_logger.error(f"Error Connecting to Mawn dbh11 Database: {e}")

    return mawn_dbh11_connection


def connect_to_mawn_supercell():
    """
    Establishes a connection to the mawndb_supercell database.

    Returns:
        psycopg2.extensions.connection: Connection object for the mawndb database.
    """
    db_info1 = config_mawn_supercell()
    mawn_supercell_connection = None  # initialize connection as None

    try:
        mawn_supercell_connection = psycopg2.connect(**db_info1)
        my_dbfiles_logger.info("Successfully connected to mawndb")
        mawn_supercell_connection.autocommit = False

    except OperationalError as e:
        my_dbfiles_logger.error(f"Error Connecting to mawn_supercell Database: {e}")

    return mawn_supercell_connection


def connect_to_mawnqc_dbh11():
    """
    Establishes a connection to the mawnqc database.

    Returns:
        psycopg2.extensions.connection: Connection object for the mawnqc dbh11 database.
    """
    db_info2 = config_mawnqc_dbh11()
    mawnqc_dbh11_connection = None  # Initialize connection as None

    try:
        mawnqc_dbh11_connection = psycopg2.connect(**db_info2)
        my_dbfiles_logger.info("Successfully connected to mawnqc")
        mawnqc_dbh11_connection.autocommit = False

    except OperationalError as e:
        my_dbfiles_logger.error("Error connecting to database", exc_info=True)

    return mawnqc_dbh11_connection


def connect_to_mawnqc_supercell():
    """
    Establishes a connection to the mawnqc database.

    Returns:
        psycopg2.extensions.connection: Connection object for the mawnqc supercell database.
    """
    db_info3 = config_mawnqc_supercell()
    mawnqc_supercell_connection = None  # Initialize connection as None

    try:
        mawnqc_supercell_connection = psycopg2.connect(**db_info3)
        my_dbfiles_logger.info("Successfully connected to mawnqc")
        mawnqc_supercell_connection.autocommit = False

    except OperationalError as e:
        my_dbfiles_logger.error("Error connecting to database", exc_info=True)

    return mawnqc_supercell_connection


def connect_to_rtma_dbh11():
    """
    Establishes a connection to the rtma_dbh11 database.

    Returns:
        psycopg2.extensions.connection: Connection object for the rtma database.
    """
    db_info4 = config_rtma_dbh11()
    rtma_dbh11_connection = None

    try:
        rtma_dbh11_connection = psycopg2.connect(**db_info4)
        my_dbfiles_logger.info("Successfully connected to rtma_dbh11 database")
        rtma_dbh11_connection.autocommit = False

    except OperationalError as e:
        my_dbfiles_logger.error(f"Error Connecting to rtma_dbh11 Database: {e}")

    return rtma_dbh11_connection


def connect_to_rtma_supercell():
    """
    Establishes a connection to the rtma database.

    Returns:
        psycopg2.extensions.connection: Connection object for the rtma database.
    """
    db_info5 = config_rtma_supercell()
    rtma_supercell_connection = None

    try:
        rtma_supercell_connection = psycopg2.connect(**db_info5)
        my_dbfiles_logger.info("Successfully connected to RTMA Supercell Database")
        rtma_supercell_connection.autocommit = False

    except OperationalError as e:
        my_dbfiles_logger.error(f"Error Connecting to RTMA Supercell Database: {e}")

    return rtma_supercell_connection


def connect_to_mawnqcl():
    """
    Establishes a connection to the mawnqcl database.

    Returns:
        psycopg2.extensions.connection: Connection object for the qctest database.
    """
    db_info6 = config_mawnqcl()
    mawnqcl_connection = None

    try:
        mawnqcl_connection = psycopg2.connect(**db_info6)
        my_dbfiles_logger.info("Successfully connected to QCL Database")
        mawnqcl_connection.autocommit = False

    except OperationalError as e:
        my_dbfiles_logger.error(f"Error Connecting to QCL Database: {e}")

    return mawnqcl_connection


def connect_to_mawnqc_test():
    """
    Establishes a connection to the qctest database.

    Returns:
        psycopg2.extensions.connection: Connection object for the qctest database.
    """
    db_info7 = config_mawnqc_test()
    mawnqc_test_connection = None

    try:
        mawnqc_test_connection = psycopg2.connect(**db_info7)
        my_dbfiles_logger.info("Successfully connected to QCTEST Database")
        mawnqc_test_connection.autocommit = False

    except OperationalError as e:
        my_dbfiles_logger.error(f"Error Connecting to QCTEST Database: {e}")

    return mawnqc_test_connection


def mawn_dbh11_cursor_connection(mawn_dbh11_connection):
    """
    Establishes a cursor connection for the mawndb database.

    Parameters:
        mawndb_dbh11_connection (psycopg2.extensions.connection): Connection object for the mawndb database.

    Returns:
        psycopg2.extras.RealDictCursor: Cursor object for the mawndb database.
    """
    try:
        mawn_dbh11_cursor = mawn_dbh11_connection.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor
        )
        my_dbfiles_logger.info(
            "Mawndb dbh11 cursor connection successfully established"
        )

    except (Exception, psycopg2.DatabaseError) as MawnDbh11CursorError:
        my_dbfiles_logger.error(
            "Error establishing mawndb cursor connection: %s", MawnDbh11CursorError
        )
        raise

    return mawn_dbh11_cursor


def mawn_supercell_cursor_connection(mawn_supercell_connection):
    """
    Establishes a cursor connection for the mawndb database.

    Parameters:
        mawndb_connection (psycopg2.extensions.connection): Connection object for the mawndb database.

    Returns:
        psycopg2.extras.RealDictCursor: Cursor object for the mawndb database.
    """
    try:
        mawn_supercell_cursor = mawn_supercell_connection.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor
        )
        my_dbfiles_logger.info("Mawndb cursor connection successfully established")

    except (Exception, psycopg2.DatabaseError) as MawnCursorError:
        my_dbfiles_logger.error(
            "Error establishing mawndb supercell cursor connection: %s", MawnCursorError
        )
        raise

    return mawn_supercell_cursor


def mawnqc_dbh11_cursor_connection(mawnqc_dbh11_connection):
    """
    Establishes a cursor connection for the mawnqc database.

    Parameters:
        mawnqc_connection (psycopg2.extensions.connection): Connection object for the mawnqc database.

    Returns:
        psycopg2.extras.RealDictCursor: Cursor object for the mawndb_qc database.
    """
    try:
        mawnqc_dbh11_cursor = mawnqc_dbh11_connection.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor
        )
        my_dbfiles_logger.info("Mawnqcl cursor connection successfully established")

    except (Exception, psycopg2.DatabaseError) as MawnqcCursorError:
        my_dbfiles_logger.error(
            "Error establishing mawnqc dbh11 cursor connection: %s", MawnqcCursorError
        )
        raise

    return mawnqc_dbh11_cursor


def mawnqc_supercell_cursor_connection(mawnqc_supercell_connection):
    """
    Establishes a cursor connection for the mawnqc supercell database.

    Parameters:
        mawndbqc_connection (psycopg2.extensions.connection): Connection object for the mawndb_qc database.

    Returns:
        psycopg2.extras.RealDictCursor: Cursor object for the mawndb_qc database.
    """
    try:
        mawnqc_supercell_cursor = mawnqc_supercell_connection.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor
        )
        my_dbfiles_logger.info("Mawnqcl cursor connection successfully established")

    except (Exception, psycopg2.DatabaseError) as MawnqcCursorError:
        my_dbfiles_logger.error(
            "Error establishing mawnqc dbh11 cursor connection: %s", MawnqcCursorError
        )
        raise

    return mawnqc_supercell_cursor


def rtma_dbh11_cursor_connection(rtma_dbh11_connection):
    """
    Establishes a cursor connection for the rtma database.

    Parameters:
        rtma_connection (psycopg2.extensions.connection): Connection object for the rtma database.

    Returns:
        psycopg2.extras.RealDictCursor: Cursor object for the rtma database.
    """
    try:
        rtma_dbh11_cursor = rtma_dbh11_connection.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor
        )
        my_dbfiles_logger.info("RTMA cursor connection successfully established")

    except (Exception, psycopg2.DatabaseError) as RtmaCursorError:
        my_dbfiles_logger.error(
            "Error establishing rtma dbh11 cursor connection: %s", RtmaCursorError
        )
        raise

    return rtma_dbh11_cursor


def rtma_supercell_cursor_connection(rtma_dbh11_connection):
    """
    Establishes a cursor connection for the rtma database.

    Parameters:
        rtma_connection (psycopg2.extensions.connection): Connection object for the rtma database.

    Returns:
        psycopg2.extras.RealDictCursor: Cursor object for the rtma database.
    """
    try:
        rtma_dbh11_cursor = rtma_dbh11_connection.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor
        )
        my_dbfiles_logger.info("RTMA cursor connection successfully established")

    except (Exception, psycopg2.DatabaseError) as RtmaCursorError:
        my_dbfiles_logger.error(
            "Error establishing rtma dbh11 cursor connection: %s", RtmaCursorError
        )
        raise

    return rtma_dbh11_cursor


def mawnqcl_cursor_connection(connect_to_mawnqcl):
    """
    Establishes a cursor connection for the rtma database.

    Parameters:
        mawnqcl_connection (psycopg2.extensions.connection): Connection object for the rtma database.

    Returns:
        psycopg2.extras.RealDictCursor: Cursor object for the rtma database.
    """
    try:
        mawnqcl_cursor = connect_to_mawnqcl.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor
        )
        my_dbfiles_logger.info("Mawnqcl cursor connection successfully established")

    except (Exception, psycopg2.DatabaseError) as MawnqclCursorError:
        my_dbfiles_logger.error(
            "Error establishing mawnqcl cursor connection: %s", MawnqclCursorError
        )
        raise

    return mawnqcl_cursor


def mawnqc_test_cursor_connection(qctest_connection):
    """
    Establishes a cursor connection for the qctest database.

    Parameters:
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
