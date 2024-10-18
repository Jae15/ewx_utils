from configparser import ConfigParser
import logging
from db_files.dbfiles_logs_config import dbfiles_logger
import sys
sys.path.append("c:/Users/mwangija/data_file/ewx_utils/ewx_utils")

# Initialize custom logger
my_dbfiles_logger = dbfiles_logger()


def config_mawn_dbh11(
    filename: str = "c:/Users/mwangija/data_file/ewx_utils/ewx_utils/database.ini",
    section: str = "mawn_dbh11",
) -> dict:
    """
    Reads database configuration credentials for mawndb from a specified ini file.

    Args:
        filename (str): Path to the ini file containing database configurations.
        section (str): Section in the ini file containing mawn_dbh11 credentials.

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

    my_dbfiles_logger.debug("mawn_dbh11 login credentials returned successfully")
    return db_info

def config_mawn_supercell(
    filename: str = "c:/Users/mwangija/data_file/ewx_utils/ewx_utils/database.ini",
    section: str = "mawn_supercell",
) -> dict:
    """
    Reads database configuration credentials for mawndb from a specified ini file.

    Args:
        filename (str): Path to the ini file containing database configurations.
        section (str): Section in the ini file containing mawn_supercell credentials.

    Returns:
        dict: A dictionary with database configuration credentials.

    Raises:
        Exception: If the specified section is not found in the ini file.
    """
    parser = ConfigParser()
    parser.read(filename)
    db_info1 = {}

    if parser.has_section(section):
        params01 = parser.items(section)
        for param01 in params01:
            db_info1[param01[0]] = param01[1]
    else:
        my_dbfiles_logger.error(f"Section {section} not found in the {filename} file")
        raise Exception(f"Section {section} not found in the {filename} file")

    my_dbfiles_logger.debug("mawn_supercell login credentials returned successfully")
    return db_info1


def config_mawnqc_dbh11(
    filename: str = "c:/Users/mwangija/data_file/ewx_utils/ewx_utils/database.ini",
    section: str = "mawnqc_dbh11",
) -> dict:
    """
    Reads database configuration credentials for mawnqc_dbh11 from the specified ini file.

    Args:
        filename (str): Path to the ini file containing database configurations.
        section (str): Section in the ini file containing mawndbqc credentials.

    Returns:
        dict: A dictionary with database configuration credentials.

    Raises:
        Exception: If the specified section is not found in the ini file.
    """
    parser = ConfigParser()
    parser.read(filename)
    db_info2 = {}

    if parser.has_section(section):
        params02 = parser.items(section)
        for param02 in params02:
            db_info2[param02[0]] = param02[1]
    else:
        my_dbfiles_logger.error(
            f"Section {section} is not found in the {filename} file"
        )
        raise Exception(f"Section {section} is not found in the {filename} file")

    my_dbfiles_logger.info("mawnqc_dbh11 login information returned successfully")
    return db_info2

def config_mawnqc_supercell(
    filename: str = "c:/Users/mwangija/data_file/ewx_utils/ewx_utils/database.ini",
    section: str = "mawnqc_supercell",
) -> dict:
    """
    Reads database configuration credentials for mawnqc_supercell from a specified ini file.

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
    db_info3 = {}

    if parser.has_section(section):
        params03 = parser.items(section)
        for param03 in params03:
            db_info3[param03[0]] = param03[1]
    else:
        my_dbfiles_logger.error(
            f"Section {section} is not found in the {filename} file"
        )
        raise Exception(f"Section {section} is not found in the {filename} file")

    my_dbfiles_logger.info("mawnqc_supercell login information returned successfully")
    return db_info3

def config_rtma_dbh11(
    filename: str = "c:/Users/mwangija/data_file/ewx_utils/ewx_utils/database.ini",
    section: str = "rtma_dbh11",
) -> dict:
    """
    Reads database configuration credentials for rtma_dbh11 from a specified ini file.

    Args:
        filename (str): Path to the ini file containing database configurations.
        section (str): Section in the ini file containing rtma_dbh11 credentials.

    Returns:
        dict: A dictionary with database configuration credentials.

    Raises:
        Exception: If the specified section is not found in the ini file.
    """
    parser = ConfigParser()
    parser.read(filename)
    db_info4 = {}

    if parser.has_section(section):
        params04 = parser.items(section)
        for param04 in params04:
            db_info4[param04[0]] = param04[1]
    else:
        my_dbfiles_logger.error(
            f"Section {section} is not found in the {filename} file"
        )
        raise Exception(f"Section {section} is not found in the {filename} file")

    my_dbfiles_logger.info("rtma_dbh11 login credentials returned successfully")
    return db_info4


def config_rtma_supercell(
    filename: str = "c:/Users/mwangija/data_file/ewx_utils/ewx_utils/database.ini",
    section: str = "rtma_supercell",
) -> dict:
    """
    Reads database configuration credentials for mawndb_rtma from a specified ini file.

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
    db_info5 = {}

    if parser.has_section(section):
        params05 = parser.items(section)
        for param05 in params05:
            db_info5[param05[0]] = param05[1]
    else:
        my_dbfiles_logger.error(
            f"Section {section} is not found in the {filename} file"
        )
        raise Exception(f"Section {section} is not found in the {filename} file")

    my_dbfiles_logger.info("RTMA supercell login credentials returned successfully")
    return db_info5


def config_mawnqc_test(
    filename: str = "c:/Users/mwangija/data_file/ewx_utils/ewx_utils/database.ini",
    section: str = "mawnqc_test",
) -> dict:
    """
    Reads database configuration credentials for mawnqc_test from a specified ini file.

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
    db_info6 = {}

    if parser.has_section(section):
        params06 = parser.items(section)
        for param06 in params06:
            db_info6[param06[0]] = param06[1]
    else:
        my_dbfiles_logger.error(
            f"Section {section} is not found in the {filename} file"
        )
        raise Exception(f"Section {section} is not found in the {filename} file")

    my_dbfiles_logger.info("QCTEST login credentials returned successfully")
    return db_info6

def config_mawnqcl(
    filename: str = "c:/Users/mwangija/data_file/ewx_utils/ewx_utils/database.ini",
    section: str = "mawnqcl",
) -> dict:
    """
    Reads database configuration credentials for mawnqc_test from a specified ini file.

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
    db_info7 = {}

    if parser.has_section(section):
        params07 = parser.items(section)
        for param07 in params07:
            db_info7[param07[0]] = param07[1]
    else:
        my_dbfiles_logger.error(
            f"Section {section} is not found in the {filename} file"
        )
        raise Exception(f"Section {section} is not found in the {filename} file")

    my_dbfiles_logger.info("QCL login credentials returned successfully")
    return db_info7

def main():
    """
    Main function to demonstrate configuration retrieval.
    """
    try:
        mawn_dbh11_config = config_mawn_dbh11()
        my_dbfiles_logger.info(f"mawn_dbh11_config: {mawn_dbh11_config}")

        mawn_supercell_config = config_mawn_supercell()
        my_dbfiles_logger.info(f"mawn_supercell_config: {mawn_supercell_config}")

        mawnqc_dbh11_config = config_mawnqc_dbh11()
        my_dbfiles_logger.info(f"mawnqc_dbh11_config: {mawnqc_dbh11_config}")

        mawnqc_supercell_config = config_mawnqc_supercell()
        my_dbfiles_logger.info(f"mawnqc_supercell_config: {mawnqc_supercell_config}")

        rtma_dbh11_config = config_rtma_dbh11()
        my_dbfiles_logger.info(f"rtma_dbh11_config: {rtma_dbh11_config}")

        rtma_supercell_config = config_rtma_supercell()
        my_dbfiles_logger.info(f"rtma_supercell_config: {rtma_supercell_config}")

        qcl_config = config_mawnqcl()
        my_dbfiles_logger.info(f"qcl_config: {qcl_config}")

        qctest_config = config_mawnqc_test()
        my_dbfiles_logger.info(f"qctest_config: {qctest_config}")

    except Exception as e:
        my_dbfiles_logger.error(f"Error in main function: {e}")


if __name__ == "__main__":
    main()
