from ewx_utils.mawndb_classes.evapotranspiration import Evapotranspiration

def test_evapo_creation_millomiters():
    evapo_obj = Evapotranspiration(25.4, "HOURLY", "MM")
    assert evapo_obj.rpetMM == 25.4
    assert evapo_obj.rpetIN == 1.0

def test_evapo_creation_inches():
    evapo_obj = Evapotranspiration(1.0, "HOURLY", "IN")
    assert evapo_obj.rpetIN == 1.0
    assert evapo_obj.rpetMM == 25.4
    
def test_evapo_invalid_creation_hourly():
    evapo_obj = Evapotranspiration(None, "HOURLY", "MM")
    assert evapo_obj.rpetMM == -9999

def test_evapo_within_valid_range_hourly():
    evapo_obj = Evapotranspiration(5, "HOURLY", "MM")
    assert evapo_obj.is_valid() == True

def test_evapo_within_valid_range_hourly_0():
    evapo_obj = Evapotranspiration(0, "HOURLY", "MM")
    assert evapo_obj.is_valid() == True

def test_evapo_outside_valid_range_hourly():
    evapo_obj = Evapotranspiration(15, "HOURLY", "MM")
    assert evapo_obj.is_valid() == False

def test_evapo_outside_valid_range_hourly_negative():
    evapo_obj = Evapotranspiration(-15, "HOURLY", "MM")
    assert evapo_obj.is_valid() == False

def test_evapo_within_valid_range_daily():
    evapo_obj = Evapotranspiration(8, "DAILY", "MM")
    assert evapo_obj.is_valid() == True

def test_evapo_outside_valid_range_daily():
    evapo_obj = Evapotranspiration(23, "DAILY", "MM")
    assert evapo_obj.is_valid() == False

def test_evapo_outside_valid_range_daily_negative():
    evapo_obj = Evapotranspiration(-23, "DAILY", "MM")
    assert evapo_obj.is_valid() == False
    

    
    
