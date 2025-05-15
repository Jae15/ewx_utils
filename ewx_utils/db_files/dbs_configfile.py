import os
import sys
import configparser
from dotenv import load_dotenv
load_dotenv()
ewx_base_path = os.getenv("EWX_BASE_PATH")
sys.path.append(ewx_base_path)
from configparser import ConfigParser, Error as ConfigParserError
from ewx_utils.ewx_config import ewx_database_configfile, ewx_log_file
from ewx_utils.logs.ewx_utils_logs_config import ewx_unstructured_logger
from ewx_utils.logs.ewx_utils_logs_config import EWXStructuredLogger

# Initialize custom logger
my_dbfiles_logger = EWXStructuredLogger(log_path=ewx_log_file)

def get_db_config(db_name: str, filename: str = ewx_database_configfile)->dict:
    """
    Fetches database configuration for a given database name fron a specified INI file

    Parameters:
        db_name (str): The name of the database, which should match the section in the configuration file
        filename(str): Path to the ini file containing database configuration
        
    Returns:
        dict: The database configuration for the given db_name.
        
    Raises:
        ValueError: If db_name is empty or None
        Exception: If the specified section is not found in the ini file
        ConfigParserError: If there is an error reading the configuration file.
        KeyError: If the specified section is not found in the configuration file.
    """
    if not db_name:
        my_dbfiles_logger.error("Database name cannot be empty or None")
        raise ValueError("Database name cannot be empty or None")
    try:
        parser = ConfigParser(interpolation=None)
        parser.read(filename)

        if parser.has_section(db_name):
            db_info = {param[0]: param[1] for param in parser.items(db_name)}
            my_dbfiles_logger.debug(f"{db_name} login credentials returned")
            #print(f"db_info: {db_info}")
            return db_info
        else:
            my_dbfiles_logger.error(f"Section {db_name} not found in the {filename} file")
            raise Exception(f"Section {db_name} not found in the {filename} file")
        
    except ConfigParserError as e:
        my_dbfiles_logger.error(f"Configuration file error for {db_name}: {str(e)}")
        raise
    except KeyError as e:
        my_dbfiles_logger.error(f"Section {db_name} not found in configuration file: {str(e)}")
        raise
    except Exception as e:
        my_dbfiles_logger.error(f"Unexpected error fetching configuration for {db_name}: {str(e)}")
        raise
    
def get_ini_section_info(ini_file_path: str) -> str:
    """
    Retrieve both the section names and their corresponding host name from the INI file.

    Parameters:
    ini_file_path (str): The path to the INI file

    Returns:
    str: A string that lists all the sections and their host names (if available) for use in the help description.
    """
    config = configparser.ConfigParser(interpolation=None)
    section_info = []

    try:
        config.read(ini_file_path)

        # Get station names
        sections = config.sections()
        section_info.append(f"Valid sections in the INI file: {' ,'.join(sections)} ")

        # Get host names for each section
        host_names = {section: config[section].get('host', 'Not specified') for section in sections}
        host_info = "\n".join([f"{section}: {host}" for section, host in host_names.items()])
        section_info.append(f"Host names in sections:\n{host_info}")

        return "\n".join(section_info)
    
    except Exception as e:
        raise FileNotFoundError(f"Error reading the INI file: {str(e)}")