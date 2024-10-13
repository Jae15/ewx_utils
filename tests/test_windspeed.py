from ewx_utils.mawndb_classes.windspeed import WindSpeed

def test_windspd_valid_creation_mps(): #meters per second
    windspd_obj = WindSpeed(10, "MPS") 
    assert windspd_obj.wspdMPS == 10  #float(10)
    assert windspd_obj.wspdMPH == 10 /  1609.344 * 3600

def test_windspd_valid_creation_mph(): #miles per hour
    windspd_obj = WindSpeed(10, "MPH") 
    assert windspd_obj.wspdMPH == 10
    assert windspd_obj.wspdMPS == 10 * 1609.344 / 3600

def test_windspd_invalid_creation_mps():
    windspd_obj = WindSpeed(None, "MPS")
    assert windspd_obj.wspdMPS == -9999
    
def test_windspd_within_valid_range():
    windspd_obj = WindSpeed(45, "MPS")
    assert windspd_obj.is_valid() == True
    
def test_windspd_within_valid_range():
    windspd_obj = WindSpeed(75, "MPS")
    assert windspd_obj.is_valid() == True
    
def test_windspd_within_valid_range():
    windspd_obj = WindSpeed(99, "MPS")
    assert windspd_obj.is_valid() == False

def test_windspd_outside_valid_range():
    windspd_obj = WindSpeed(100, "MPS")
    assert windspd_obj.is_valid() == False

def test_windspd_outside_valid_range():
    windspd_obj = WindSpeed(130, "MPS")
    assert windspd_obj.is_valid() == False
    
