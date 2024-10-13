from ewx_utils.mawndb_classes.winddirection import WindDirection

def test_winddir_within_valid_range():
    winddir_obj = WindDirection(198, "DEGREES")
    assert winddir_obj.is_valid() == True

def test_winddir_outside_valid_range():
    winddir_obj = WindDirection(450, "DEGREES")
    assert winddir_obj.is_valid() == False
    
def test_winddir_invalid_creation():
    winddir_obj = WindDirection(None, "DEGREES")
    assert winddir_obj.wdirDEGREES == -9999

def test_winddir_invalid_compass_direction():
    winddir_obj = WindDirection(-20, "DEGREES")
    assert winddir_obj.wdirCOMPASS == "NA"
    
def test_winddir_valid_compass_direction():
    winddir_obj = WindDirection(45, "DEGREES")
    assert winddir_obj.wdirCOMPASS == "NE"

def test_winddir_invalid_compass_input():
    winddir_obj = WindDirection(-90, "DEGREES")
    assert winddir_obj.wdirCOMPASS == "NA"

def test_winddir_valid_compass_direction():
    winddir_obj = WindDirection(11.25, "DEGREES")
    assert winddir_obj.wdirCOMPASS == "NNE"
    
def test_winddir_valid_compass_input():
    winddir_obj = WindDirection(33.75, "DEGREES")
    assert winddir_obj.wdirCOMPASS == "NE"

def test_winddir_valid_compass_input():
    winddir_obj = WindDirection(56.25, "DEGREES")
    assert winddir_obj.wdirCOMPASS == "ENE"
    
def test_winddir_valid_compass_input():
    winddir_obj = WindDirection(78.75, "DEGREES")
    assert winddir_obj.wdirCOMPASS == "E"
    
def test_winddir_valid_compass_input():
    winddir_obj = WindDirection(101.25, "DEGREES")
    assert winddir_obj.wdirCOMPASS == "ESE"
    
def test_winddir_valid_compass_input():
    winddir_obj = WindDirection(123.75, "DEGREES")
    assert winddir_obj.wdirCOMPASS == "SE"

def test_winddir_valid_compass_input():
    winddir_obj = WindDirection(146.25, "DEGREES")
    assert winddir_obj.wdirCOMPASS == "SSE"

def test_winddir_valid_compass_input():
    winddir_obj = WindDirection(168.75, "DEGREES")
    assert winddir_obj.wdirCOMPASS == "S"

def test_winddir_valid_compass_input():
    winddir_obj = WindDirection(191.25, "DEGREES")
    assert winddir_obj.wdirCOMPASS == "SSW"

def test_winddir_valid_compass_input():
    winddir_obj = WindDirection(213.75, "DEGREES")
    assert winddir_obj.wdirCOMPASS == "SW"

def test_winddir_valid_compass_input():
    winddir_obj = WindDirection(236.25, "DEGREES")
    assert winddir_obj.wdirCOMPASS == "WSW"
    
def test_winddir_valid_compass_input():
    winddir_obj = WindDirection(258.75, "DEGREES")
    assert winddir_obj.wdirCOMPASS == "W"

def test_winddir_valid_compass_input():
    winddir_obj = WindDirection(281.25, "DEGREES")
    assert winddir_obj.wdirCOMPASS == "WNW"

def test_winddir_valid_compass_input():
    winddir_obj = WindDirection(303.75, "DEGREES")
    assert winddir_obj.wdirCOMPASS == "NW"

def test_winddir_valid_compass_input():
    winddir_obj = WindDirection(326.25, "DEGREES")
    assert winddir_obj.wdirCOMPASS == "NNW"

def test_winddir_valid_compass_input():
    winddir_obj = WindDirection(348.75, "DEGREES")
    assert winddir_obj.wdirCOMPASS == "N"


