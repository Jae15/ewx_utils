from configparser import ConfigParser
import logging
from db_files.dbfiles_logs_config import dbfiles_logger

# Initialize custom logger
my_dbfiles_logger = dbfiles_logger()


def config_mawndb(
    filename: str = "c:/Users/mwangija/data_file/ewx_utils/ewx_utils/database.ini",
    section: str = "mawndb",
) -> dict:
    """
    Reads database configuration for mawndb from a specified ini file.

    Args:
        filename (str): Path to the ini file containing database configurations.
        section (str): Section in the ini file containing mawndb credentials.

    Returns:
        dict: A dictionary with database configuration credentials.

    Raises:
        Exception: If the specified section is not found in the ini file.
    """
    parser = ConfigParser()
    parser.read(filename)
    db_info = {}

    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db_info[param[0]] = param[1]
    else:
        my_dbfiles_logger.error(f"Section {section} not found in the {filename} file")
        raise Exception(f"Section {section} not found in the {filename} file")

    my_dbfiles_logger.debug("Mawndb login credentials returned successfully")
    return db_info


def config_mawndbqc(
    filename: str = "c:/Users/mwangija/data_file/ewx_utils/ewx_utils/database.ini",
    section: str = "mawndb_qcl",
) -> dict:
    """
    Reads database configuration for mawndb_qcl from a specified ini file.

    Args:
        filename (str): Path to the ini file containing database configurations.
        section (str): Section in the ini file containing mawndb_qcl credentials.

    Returns:
        dict: A dictionary with database configuration credentials.

    Raises:
        Exception: If the specified section is not found in the ini file.
    """
    parser = ConfigParser()
    parser.read(filename)
    db_info2 = {}

    if parser.has_section(section):
        params01 = parser.items(section)
        for param01 in params01:
            db_info2[param01[0]] = param01[1]
    else:
        my_dbfiles_logger.error(
            f"Section {section} is not found in the {filename} file"
        )
        raise Exception(f"Section {section} is not found in the {filename} file")

    my_dbfiles_logger.info("Mawnqcl login information returned successfully")
    return db_info2


def config_mawndbrtma(
    filename: str = "c:/Users/mwangija/data_file/ewx_utils/ewx_utils/database.ini",
    section: str = "mawndb_rtma",
) -> dict:
    """
    Reads database configuration for mawndb_rtma from a specified ini file.

    Args:
        filename (str): Path to the ini file containing database configurations.
        section (str): Section in the ini file containing mawndb_rtma credentials.

    Returns:
        dict: A dictionary with database configuration credentials.

    Raises:
        Exception: If the specified section is not found in the ini file.
    """
    parser = ConfigParser()
    parser.read(filename)
    db_info3 = {}

    if parser.has_section(section):
        params02 = parser.items(section)
        for param02 in params02:
            db_info3[param02[0]] = param02[1]
    else:
        my_dbfiles_logger.error(
            f"Section {section} is not found in the {filename} file"
        )
        raise Exception(f"Section {section} is not found in the {filename} file")

    my_dbfiles_logger.info("RTMA login credentials returned successfully")
    return db_info3


def config_qctest(
    filename: str = "c:/Users/mwangija/data_file/ewx_utils/ewx_utils/database.ini",
    section: str = "mawnqc_test",
) -> dict:
    """
    Reads database configuration for mawnqc_test from a specified ini file.

    Args:
        filename (str): Path to the ini file containing database configurations.
        section (str): Section in the ini file containing mawnqc_test credentials.

    Returns:
        dict: A dictionary with database configuration credentials.

    Raises:
        Exception: If the specified section is not found in the ini file.
    """
    parser = ConfigParser()
    parser.read(filename)
    db_info4 = {}

    if parser.has_section(section):
        params03 = parser.items(section)
        for param03 in params03:
            db_info4[param03[0]] = param03[1]
    else:
        my_dbfiles_logger.error(
            f"Section {section} is not found in the {filename} file"
        )
        raise Exception(f"Section {section} is not found in the {filename} file")

    my_dbfiles_logger.info("QCTEST login credentials returned successfully")
    return db_info4


def main():
    """
    Main function to demonstrate configuration retrieval.
    """
    try:
        mawndb_config = config_mawndb()
        my_dbfiles_logger.info(f"mawndb_config: {mawndb_config}")

        mawndbqc_config = config_mawndbqc()
        my_dbfiles_logger.info(f"mawndbqc_config: {mawndbqc_config}")

        rtma_config = config_mawndbrtma()
        my_dbfiles_logger.info(f"rtma_config: {rtma_config}")

        qctest_config = config_qctest()
        my_dbfiles_logger.info(f"qctest_config: {qctest_config}")

    except Exception as e:
        my_dbfiles_logger.error(f"Error in main function: {e}")


if __name__ == "__main__":
    main()
