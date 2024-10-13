from ewx_utils.db_files.dbconnection import connect_to_mawn_dbh11
from ewx_utils.db_files.dbconnection import (
    connect_to_mawn_dbh11, connect_to_mawn_supercell,
    connect_to_mawnqc_dbh11, connect_to_mawnqc_supercell,
    connect_to_mawnqcl, connect_to_rtma_dbh11,
    connect_to_rtma_supercell, connect_to_mawnqc_test
)
from ewx_utils.db_files.dbconnection import (
    mawn_dbh11_cursor_connection, mawn_supercell_cursor_connection,
    mawnqc_dbh11_cursor_connection, mawnqc_supercell_cursor_connection,
    mawnqcl_cursor_connection, rtma_dbh11_cursor_connection,
    rtma_supercell_cursor_connection, mawnqc_test_cursor_connection
)

def test_connect_to_mawn_dbh11():
    mawn_dbh11_connection = connect_to_mawn_dbh11()
    assert mawn_dbh11_connection is not None

def test_connect_to_mawn_supercell():
    mawn_supercell_connection = connect_to_mawn_supercell()
    assert mawn_supercell_connection is not None

def test_connect_to_mawnqc_dbh11():
    mawnqc_dbh11_connection = connect_to_mawnqc_dbh11()
    assert mawnqc_dbh11_connection is not None

def test_connect_to_mawnqc_supercell():
    mawnqc_supercell_connection = connect_to_mawnqc_supercell()
    assert mawnqc_supercell_connection is not None

def test_connect_to_mawnqcl():
    mawnqcl_connection = connect_to_mawnqcl()
    assert mawnqcl_connection is not None

def test_connect_to_rtma_dbh11():
    rtma_dbh11_connection = connect_to_rtma_dbh11()
    assert rtma_dbh11_connection is not None

def test_connect_to_rtma_supercell():
    rtma_supercell_connection = connect_to_rtma_supercell()
    assert rtma_supercell_connection is not None

def test_connect_to_mawnqc_test():
    mawnqc_test_connection = connect_to_mawnqc_test()
    assert mawnqc_test_connection is not None

def test_get_cursor_mawn_dbh11():
    mawn_dbh11_connection = connect_to_mawn_dbh11()
    mawn_dbh11_cursor = mawn_dbh11_cursor_connection(mawn_dbh11_connection)
    assert mawn_dbh11_cursor is not None
    
def test_get_cursor_mawn_supercell():
    mawn_supercell_connection = connect_to_mawn_supercell()
    mawn_supercell_cursor = mawn_supercell_cursor_connection(mawn_supercell_connection)
    assert mawn_supercell_cursor is not None

def test_get_cursor_mawnqc_dbh11():
    mawnqc_dbh11_connection = connect_to_mawnqc_dbh11()
    mawnqc_cursor = mawnqc_dbh11_cursor_connection(mawnqc_dbh11_connection)
    assert mawnqc_cursor is not None

def test_get_cursor_mawnqc_supercell():
    mawnqc_supercell_connection = connect_to_mawn_supercell()
    mawnqc_supercell_cursor = mawnqc_supercell_cursor_connection(mawnqc_supercell_connection)
    assert mawnqc_supercell_cursor is not None

def test_get_cursor_mawn_supercell():
    mawn_supercell_connection = connect_to_mawn_supercell()
    mawndbqc_cursr = mawn_supercell_cursor_connection(mawn_supercell_connection)
    assert mawndbqc_cursr is not None
    
def test_get_cursor_mawnqcl():
    mawnqcl_dbh11_connection = connect_to_mawnqcl()
    mawnqcl_cursor = mawnqcl_cursor_connection(mawnqcl_dbh11_connection)
    assert mawnqcl_cursor is not None

def test_get_cursor_rtma_dbh11():
    rtma_dbh11_connection = connect_to_rtma_dbh11()
    rtma_dbh11_cursor = rtma_dbh11_cursor_connection(rtma_dbh11_connection)
    assert rtma_dbh11_cursor is not None
    
def test_get_cursor_rtma_supercell():
    rtma_supercell_connection = connect_to_rtma_supercell()
    rtma_supercell_cursor = rtma_supercell_cursor_connection(rtma_supercell_connection)
    assert rtma_supercell_cursor is not None

def test_get_cursor_mawnqc_test():
    mawnqc_test_connection = connect_to_mawnqc_test()
    mawnqc_test_cursor = mawnqc_test_cursor_connection(mawnqc_test_connection)
    assert mawnqc_test_cursor is not None
