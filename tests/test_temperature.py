#import sys
from ewx_utils.mawndb_classes.temperature import temperature
import datetime

def test_temp_creation_celcius():
    temp_obj = temperature(25, "C")
    assert temp_obj.tempC == 25
    assert temp_obj.tempF == 77
    assert temp_obj.tempK == 298.15
    
def test_temp_creation_fahrenheit():
    temp_obj = temperature(77, "F")
    assert temp_obj.tempC == 25
    assert temp_obj.tempF == 77
    assert temp_obj.tempK == 298.15

def test_temp_creation_kelvin():
    temp_obj = temperature(298.15, "K")
    assert temp_obj.tempC == 25
    assert temp_obj.tempF == 77
    assert temp_obj.tempK == 298.15

def test_temp_invalid_creation():
    temp_obj = temperature(None, "C")
    assert temp_obj.tempC == None
    assert temp_obj.tempF == None
    assert temp_obj.tempK == None
    
def test_temp_within_valid_range():
    valid_date = datetime.datetime(2023, 4, 22)
    temp_obj = temperature(28, "C", valid_date)
    assert temp_obj.record_date != None
    assert temp_obj.IsValid() == True

def test_temp_in_invalid_range():
    invalid_date = None
    temp_obj = temperature(100, "C", invalid_date)
    assert temp_obj.IsValid() == False

def test_for_invalid_temp():
    valid_date = datetime.datetime(2023, 2, 20)
    temp_obj = temperature(-100, "C", valid_date)
    assert temp_obj.IsValid() == False
    
    
