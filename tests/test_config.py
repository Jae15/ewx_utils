from ewx_utils.db_files.configfile import (
    config_mawn_dbh11, config_mawn_supercell, 
    config_mawnqc_dbh11, config_mawnqc_supercell,
    config_rtma_dbh11, config_rtma_supercell,
    config_mawnqc_test, config_mawnqcl
)

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

