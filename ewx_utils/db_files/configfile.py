from configparser import ConfigParser
import logging
from os import lseek

logging.basicConfig(filename = 'log_file.log', level = logging.DEBUG,
                    format = '%(asctime)s, %(levelname)s, %(message)s')
logging.debug('This is a debug message')
logging.info('This is an info message')
logging.warning('This is a warning message')
logging.error('This is an error message')
logging.critical('This is a critical message')


#Import the filename and database name from the database.ini file for mawndb
#def config_mawndb(filename = "database.ini", section = "mawndb"):
def config_mawndb(filename = "c:/Users/mwangija/data_file/ewx_utils/ewx_utils/database.ini", section = "mawndb"):
    parser = ConfigParser()
    print(parser)
    
    #Use the parser to read the database.ini file
    parser.read(filename)
    
    print({s: dict(parser[s]) for s in parser.sections()})
    print(parser)
    #Create an empty dictionary db_info to store the configuration credentials for mawndb
    db_info = {}
    
    print(parser.has_section("mawndb"))
    #Loop through the section and collect the conguration credentials for mawndb and store them in the dictionary db_info
    if parser.has_section(section):
        params = parser.items(section) #Use the .items() dictionary method to obtain the credentials since they're stored as (key,value) pairs
        for param in params:
            db_info[param[0]] = param[1] # index 0: key and index 1: value
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    logging.debug("db_info returned successfully")
    
    return(db_info)

config_mawndb()
    
#Import the filename and database name from the database.ini file for mawndb_qcl
#def config_mawndbqc(filename = "database.ini", section = "mawndb_qc"):
def config_mawndbqc(filename = "c:/Users/mwangija/data_file/ewx_utils/ewx_utils/database.ini", section = "mawndb_qcl"):
        # creating a parser
        parser = ConfigParser()
        # reading the config file
        parser.read(filename)
        #print(parser)
        # Create an empty dictionary db_info2 to store the configuration credentials for mawndb_qc
        db_info2 = {}
        # Loop through the section and collect the conguration credentials for mawndb_qc and store them in the dictionary db_info2
        if parser.has_section(section):
            params01 = parser.items(section) # Use the .items() dictionary method to obtain the credentials since they're stored as (key,value) pairs
            for param01 in params01:
                db_info2[param01[0]] = param01[1] # index 0: key and index 1: value

        else:
            raise Exception('Section {0} is not found in the {1} file.'.format(section, filename))
        
        return(db_info2)
        #print(db2)

config_mawndbqc()

"""
except Exception as e:
logging.error('Error reading mawndb configuration: %s', exc_info = e)
return None
except Exception as e:
logging.error('Error reading mawndb_qc database configuration: %s', exc_info = e)
return None

"""



