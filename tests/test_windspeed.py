#import sys
#import pytest
#sys.path.append('c:/Users/mwangija/data_file/mwangija_missing_data')
from ewx_utils.mawndb_classes.windspeed import windspeed

def test_windspd_valid_creation_mps(): #meters per second
    windspd_obj = windspeed(10, "MPS") 
    assert windspd_obj.wspdMPS == 10  #float(10)
    assert windspd_obj.wspdMPH == 10 /  1609.344 * 3600

def test_windspd_valid_creation_mph(): #miles per hour
    windspd_obj = windspeed(10, "MPH") 
    assert windspd_obj.wspdMPH == 10
    assert windspd_obj.wspdMPS == 10 * 1609.344 / 3600

def test_windspd_invalid_creation_mps():
    windspd_obj = windspeed(None, "MPS")
    assert windspd_obj.wspdMPS == -9999
    
def test_windspd_within_valid_range():
    windspd_obj = windspeed(45, "MPS")
    assert windspd_obj.IsValid() == True
    
def test_windspd_within_valid_range():
    windspd_obj = windspeed(75, "MPS")
    assert windspd_obj.IsValid() == True
    
def test_windspd_within_valid_range():
    windspd_obj = windspeed(99, "MPS")
    assert windspd_obj.IsValid() == False

def test_windspd_outside_valid_range():
    windspd_obj = windspeed(100, "MPS")
    assert windspd_obj.IsValid() == False

def test_windspd_outside_valid_range():
    windspd_obj = windspeed(130, "MPS")
    assert windspd_obj.IsValid() == False
    
