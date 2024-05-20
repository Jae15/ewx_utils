import sys
import pytest
#sys.path.append('c:/Users/mwangija/data_file/mwangija_missing_data')
from ewx_utils.mawndb_classes.winddirection import winddirection

def test_winddir_within_valid_range():
    winddir_obj = winddirection(198, "DEGREES")
    assert winddir_obj.IsValid() == True

def test_winddir_outside_valid_range():
    winddir_obj = winddirection(450, "DEGREES")
    assert winddir_obj.IsValid() == False
    
def test_winddir_invalid_creation():
    winddir_obj = winddirection(None, "DEGREES")
    assert winddir_obj.wdirDEGREES == -9999

def test_winddir_invalid_compass_direction():
    winddir_obj = winddirection(-20, "DEGREES")
    assert winddir_obj.wdirCOMPASS == "NA"
    
def test_winddir_valid_compass_direction():
    winddir_obj = winddirection(45, "DEGREES")
    assert winddir_obj.wdirCOMPASS == "NE"

def test_winddir_invalid_compass_input():
    winddir_obj = winddirection(-90, "DEGREES")
    assert winddir_obj.wdirCOMPASS == "NA"

def test_winddir_valid_compass_direction():
    winddir_obj = winddirection(11.25, "DEGREES")
    assert winddir_obj.wdirCOMPASS == "NNE"
    
def test_winddir_valid_compass_input():
    winddir_obj = winddirection(33.75, "DEGREES")
    assert winddir_obj.wdirCOMPASS == "NE"

def test_winddir_valid_compass_input():
    winddir_obj = winddirection(56.25, "DEGREES")
    assert winddir_obj.wdirCOMPASS == "ENE"
    
def test_winddir_valid_compass_input():
    winddir_obj = winddirection(78.75, "DEGREES")
    assert winddir_obj.wdirCOMPASS == "E"
    
def test_winddir_valid_compass_input():
    winddir_obj = winddirection(101.25, "DEGREES")
    assert winddir_obj.wdirCOMPASS == "ESE"
    
def test_winddir_valid_compass_input():
    winddir_obj = winddirection(123.75, "DEGREES")
    assert winddir_obj.wdirCOMPASS == "SE"

def test_winddir_valid_compass_input():
    winddir_obj = winddirection(146.25, "DEGREES")
    assert winddir_obj.wdirCOMPASS == "SSE"

def test_winddir_valid_compass_input():
    winddir_obj = winddirection(168.75, "DEGREES")
    assert winddir_obj.wdirCOMPASS == "S"

def test_winddir_valid_compass_input():
    winddir_obj = winddirection(191.25, "DEGREES")
    assert winddir_obj.wdirCOMPASS == "SSW"

def test_winddir_valid_compass_input():
    winddir_obj = winddirection(213.75, "DEGREES")
    assert winddir_obj.wdirCOMPASS == "SW"

def test_winddir_valid_compass_input():
    winddir_obj = winddirection(236.25, "DEGREES")
    assert winddir_obj.wdirCOMPASS == "WSW"
    
def test_winddir_valid_compass_input():
    winddir_obj = winddirection(258.75, "DEGREES")
    assert winddir_obj.wdirCOMPASS == "W"

def test_winddir_valid_compass_input():
    winddir_obj = winddirection(281.25, "DEGREES")
    assert winddir_obj.wdirCOMPASS == "WNW"

def test_winddir_valid_compass_input():
    winddir_obj = winddirection(303.75, "DEGREES")
    assert winddir_obj.wdirCOMPASS == "NW"

def test_winddir_valid_compass_input():
    winddir_obj = winddirection(326.25, "DEGREES")
    assert winddir_obj.wdirCOMPASS == "NNW"

def test_winddir_valid_compass_input():
    winddir_obj = winddirection(348.75, "DEGREES")
    assert winddir_obj.wdirCOMPASS == "N"


