#import pytest
import sys
#sys.path.append('c:/Users/mwangija/data_file/mwangija_missing_data')
from ewx_utils.db_files.dbconnection import connect_to_mawndb
from ewx_utils.db_files.dbconnection import connect_to_mawndbqc
from ewx_utils.db_files.dbconnection import mawndb_cursor_connection
from ewx_utils.db_files.dbconnection import mawnqc_cursor_connection

def test_connect_to_mawndb():
    mawndb_connection = connect_to_mawndb()
    assert mawndb_connection is not None

def test_connect_to_mawndbqc():
    mawndbqc_connection = connect_to_mawndbqc
    assert mawndbqc_connection is not None

def test_get_cursor_mawndb():
    mawndb_connection = connect_to_mawndb()
    mawndb_cursr = mawndb_cursor_connection(mawndb_connection)
    assert mawndb_cursr is not None
    
def test_get_cursor_mawndbqc():
    mawnqc_connection = connect_to_mawndbqc()
    mawndbqc_cursr = mawnqc_cursor_connection(mawnqc_connection)
    assert mawndbqc_cursr is not None
    
