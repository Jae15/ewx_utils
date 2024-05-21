from configparser import ConfigParser
import logging
from os import lseek

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)s - %(asctime)s - %(messages)s')
file_handler = logging.FileHandler('log_file.log')
file_handler.setLevel(logging.DEBUG)
logger.addHandler(file_handler)

"""
logging.basicConfig(filename = 'log_file.log', level = logging.DEBUG,
                    format = '%(asctime)s, %(levelname)s, %(message)s')
logging.debug('This is a debug message')
logging.info('This is an info message')
logging.warning('This is a warning message')
logging.error('This is an error message')
logging.critical('This is a critical message')

"""

#Import the filename and database name from the database.ini file for mawndb
#def config_mawndb(filename = "database.ini", section = "mawndb"):
def config_mawndb(filename = "c:/Users/mwangija/data_file/ewx_utils/ewx_utils/database.ini", section = "mawndb"):
    parser = ConfigParser()
    
    #Use the parser to read the database.ini file
    parser.read(filename)
    
    #Create an empty dictionary db_info to store the configuration credentials for mawndb
    db_info = {}
    
    #Loop through the section and collect the conguration credentials for mawndb and store them in the dictionary db_info
    if parser.has_section(section):
        params = parser.items(section) #Use the .items() dictionary method to obtain the credentials since they're stored as (key,value) pairs
        for param in params:
            db_info[param[0]] = param[1] # index 0: key and index 1: value
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    logging.debug("Mawndb login credentials returned successfully")
    
    return(db_info)
    
#Import the filename and database name from the database.ini file for mawndb_qcl
#def config_mawndbqc(filename = "database.ini", section = "mawndb_qc"):
def config_mawndbqc(filename = "c:/Users/mwangija/data_file/ewx_utils/ewx_utils/database.ini", section = "mawndb_qcl"):
        # creating a parser
        parser = ConfigParser()
        
        parser.read(filename)
        
        # Create an empty dictionary db_info2 to store the configuration credentials for mawndb_qc
        db_info2 = {}

        # Loop through the section and collect the conguration credentials for mawndb_qc and store them in the dictionary db_info2
        if parser.has_section(section):
            params01 = parser.items(section) # Use the .items() dictionary method to obtain the credentials since they're stored as (key,value) pairs
            for param01 in params01:
                db_info2[param01[0]] = param01[1] # index 0: key and index 1: value

        else:
            raise Exception('Section {0} is not found in the {1} file.'.format(section, filename))
        logging.DEBUG("Mawnqc login information returned successfully")
        
        return(db_info2)
        #print(db2)


"""
except Exception as e:
logging.error('Error reading mawndb configuration: %s', exc_info = e)
return None
except Exception as e:
logging.error('Error reading mawndb_qc database configuration: %s', exc_info = e)
return None

"""



