#import sys
#sys.path.append('c:/Users/mwangija/data_file/mwangija_missing_data')
from ewx_utils.mawndb_classes.precipitation import precipitation
                                
def test_precip_creation_millimiters():
    precip_obj = precipitation(25.4,"HOURLY","MM")
    assert precip_obj.pcpnMM == 25.4
    assert precip_obj.pcpnIN == 1.0
    
def test_precip_creation_inches():
    precip_obj = precipitation(1.0,"HOURLY", "IN")
    assert precip_obj.pcpnMM == 25.4
    assert precip_obj.pcpnIN == 1.0

def test_precip_invalid_creation():
    precip_obj = precipitation(None, "HOURLY", "MM")
    assert precip_obj.pcpnMM == -9999
    
def test_precip_within_valid_range_hourly():
    precip_obj = precipitation(20, "HOURLY", "MM")
    assert precip_obj.IsValid() == True

def test_precip_outside_valid_range_hourly():
    precip_obj = precipitation(100, "HOURLY", "MM")
    assert precip_obj.IsValid() == False
    
def test_precip_within_valid_range_fivemin():
    precip_obj = precipitation(5, "FIVEMIN", "MM")
    assert precip_obj.IsValid() == True
    
def test_precip_outside_valid_range_fivemin():
    precip_obj = precipitation(50, "FIVEMIN", "MM")
    assert precip_obj.IsValid() == False
    
def test_precip_within_valid_range_daily():
    precip_obj = precipitation(150, "DAILY", "MM")
    assert precip_obj.IsValid() == True
    
def test_precip_outside_valid_range_daily():
    precip_obj = precipitation(300, "DAILY", "MM")
    assert precip_obj.IsValid() == False
    
    

    
    


