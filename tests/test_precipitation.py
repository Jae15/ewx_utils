from ewx_utils.mawndb_classes.precipitation import Precipitation
                                
def test_precip_creation_millimiters():
    precip_obj = Precipitation(25.4,"HOURLY","MM")
    assert precip_obj.pcpnMM == 25.4
    assert precip_obj.pcpnIN == 1.0
    
def test_precip_creation_inches():
    precip_obj = Precipitation(1.0,"HOURLY", "IN")
    assert precip_obj.pcpnMM == 25.4
    assert precip_obj.pcpnIN == 1.0

def test_precip_invalid_creation():
    precip_obj = Precipitation(None, "HOURLY", "MM")
    assert precip_obj.pcpnMM == -9999
    
def test_precip_within_valid_range_hourly():
    precip_obj = Precipitation(20, "HOURLY", "MM")
    assert precip_obj.is_valid() == True

def test_precip_within_valid_range_hourly_0():
    precip_obj = Precipitation(0, "HOURLY", "MM")
    assert precip_obj.is_valid() == True

def test_precip_outside_valid_range_hourly():
    precip_obj = Precipitation(100, "HOURLY", "MM")
    assert precip_obj.is_valid() == False

def test_precip_outside_valid_range_hourlyn():
    precip_obj = Precipitation(-100, "HOURLY", "MM")
    assert precip_obj.is_valid() == False
    
def test_precip_within_valid_range_fivemin():
    precip_obj = Precipitation(5, "FIVEMIN", "MM")
    assert precip_obj.is_valid() == True
    
def test_precip_outside_valid_range_fivemin():
    precip_obj = Precipitation(50, "FIVEMIN", "MM")
    assert precip_obj.is_valid() == False

def test_precip_outside_valid_range_fivemin():
    precip_obj = Precipitation(-50, "FIVEMIN", "MM")
    assert precip_obj.is_valid() == False
    
def test_precip_within_valid_range_daily():
    precip_obj = Precipitation(150, "DAILY", "MM")
    assert precip_obj.is_valid() == True
    
def test_precip_outside_valid_range_daily():
    precip_obj = Precipitation(300, "DAILY", "MM")
    assert precip_obj.is_valid() == False

def test_precip_outside_valid_range_daily():
    precip_obj = Precipitation(-100, "DAILY", "MM")
    assert precip_obj.is_valid() == False
    
    

    
    


