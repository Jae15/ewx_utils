#import sys
#import pytest
#sys.path.append('c:/Users/mwangija/data_file/mwangija_missing_data/ewx_utils')
from ewx_utils.db_files.configfile import config_mawndb
from ewx_utils.db_files.configfile import config_mawndbqc

#from db_files.dbconnection import dbconnection
"""
def test_config_section_found_mawndb():
    expected_config_details_mawndb = {
        'host' :'127.0.0.1',
        'port' : '15503',
        'dbname' : 'mawndb',
        'user' : 'mwangija',
        'password' : 'xuPQb#batNT$'
    }
    assert config_mawndb('database.ini', 'mawndb') == expected_config_details_mawndb
    
def test_config_section_found_mawndbqc():
    expected_config_details_mawndbqc = {
        'host' : 'localhost',
        'port' : '5432',
        'dbname' : 'mawndb_qcl',
        'user' : 'postgres',
        'password' : 'postgres'
    }
    assert config_mawndbqc('database.ini', 'mawndb_qcl') == expected_config_details_mawndbqc
    
"""

