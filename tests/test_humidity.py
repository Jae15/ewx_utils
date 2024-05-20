#import sys
#sys.path.append('c:/Users/mwangija/data_file/mwangija_missing_data')
#print(sys.path)
#from mawndb_classes.humidity import humidity
from ewx_utils.mawndb_classes.humidity import humidity
#import pytest
import datetime

def test_hum_init():
    hum = humidity(50, "PCT")
    assert hum.relhPCT == 50
    assert hum.relhDEC == 0.5

def test_hum_is_valid():
    hum = humidity(50, "PCT")
    assert hum.IsValid() == True
    
def test_hum_invalid():
    hum = humidity(200, "PCT")
    assert hum.IsValid() == False

def test_hum_in_range():
    hum = humidity(103, "PCT")
    assert hum.IsInRange() == (100, "RELH_CAP")

def test_hum_not_in_range():
    hum = humidity(110, "PCT")
    assert hum.IsInRange() == (None, None)
