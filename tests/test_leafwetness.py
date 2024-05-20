#import sys
from ewx_utils.mawndb_classes.leafwetness import leafwetness

def test_leafwt_hourly_leaf0_valid():
    leafwt_obj = leafwetness(0.0, "HOURLY", "LEAF0")
    assert leafwt_obj.IsValid() == True

def test_leafwt_hourly_leaf0_valid():
    leafwt_obj = leafwetness(0.75, "HOURLY", "LEAF0")
    assert leafwt_obj.IsValid() == True
    
def test_leafwt_hourly_leaf0_valid():
    leafwt_obj = leafwetness(1.0, "HOURLY", "LEAF0")
    assert leafwt_obj.IsValid() == True
    
def test_leafwt_hourly_leaf0_invalid():
    leafwt_obj = leafwetness(1.5, "HOURLY", "LEAF0")
    assert leafwt_obj.IsValid() == False
    
def test_leafwt_hourly_leaf1_valid():
    leafwt_obj = leafwetness(0.9, "HOURLY", "LEAF1")
    assert leafwt_obj.IsValid() == True

def test_leafwt_hourly_leaf1_valid():
    leafwt_obj = leafwetness(1.0, "HOURLY", "LEAF1")
    assert leafwt_obj.IsValid() == True

def test_leafwt_hourly_leaf1_invalid():
    leafwt_obj = leafwetness(2.2, "HOURLY", "LEAF1")
    assert leafwt_obj.IsValid() == False

def test_leafwt_hourly_leaf0_wet():
    leafwt_obj = leafwetness(0.5, "HOURLY", "LEAF0")
    assert leafwt_obj.IsWet() == True

def test_leafwt_hourly_leaf0_dry():
    leafwt_obj = leafwetness(0.1, "HOURLY", "LEAF1")
    assert leafwt_obj.IsWet() == False
    
def test_leafwt_fivemin_leaf0_valid():
    leafwt_obj = leafwetness(0.1, "FIVEMIN", "LEAF0")
    assert leafwt_obj.IsValid() == True

def test_leafwt_fivemin_leaf0_valid():
    leafwt_obj = leafwetness(1.9, "FIVEMIN", "LEAF0")
    assert leafwt_obj.IsValid() == True
    
def test_leafwt_fivemin_leaf1_invalid():
    leafwt_obj = leafwetness(-135, "FIVEMIN", "LEAF0")
    assert leafwt_obj.IsValid() == False
    
def test_leafwt_fivemin_leaf0_wet():
    leafwt_obj = leafwetness(0.3, "FIVEMIN", "LEAF0")
    assert leafwt_obj.IsValid() == True

def test_leafwt_fivemin_leaf0_dry():
    leafwt_obj = leafwetness(9999,"FIVEMIN", "LEAF1")
    assert leafwt_obj.IsWet() == False





