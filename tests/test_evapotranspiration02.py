#import sys
#import pytest
#sys.path.append('c:/Users/mwangija/data_file/mwangija_missing_data')
from ewx_utils.mawndb_classes.evapotranspiration import evapotranspiration

def test_evapo_creation_millomiters():
    evapo_obj = evapotranspiration(25.4, "HOURLY", "MM")
    assert evapo_obj.rpetMM == 25.4
    assert evapo_obj.rpetIN == 1.0

def test_evapo_creation_inches():
    evapo_obj = evapotranspiration(1.0, "HOURLY", "IN")
    assert evapo_obj.rpetIN == 1.0
    assert evapo_obj.rpetMM == 25.4
    
def test_evapo_invalid_creation_hourly():
    evapo_obj = evapotranspiration(None, "HOURLY", "MM")
    assert evapo_obj.rpetMM == -9999

def test_evapo_within_valid_range_hourly():
    evapo_obj = evapotranspiration(5, "HOURLY", "MM")
    assert evapo_obj.IsValid() == True
    
def test_evapo_outside_valid_range_hourly():
    evapo_obj = evapotranspiration(15, "HOURLY", "MM")
    assert evapo_obj.IsValid() == False

def test_evapo_within_valid_range_daily():
    evapo_obj = evapotranspiration(8, "DAILY", "MM")
    assert evapo_obj.IsValid() == True

def test_evapo_outside_valid_range_daily():
    evapo_obj = evapotranspiration(23, "DAILY", "MM")
    assert evapo_obj.IsValid() == False
    

    
    
