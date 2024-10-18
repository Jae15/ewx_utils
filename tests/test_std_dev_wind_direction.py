from ewx_utils.mawndb_classes.std_dev_wind_direction import StdDevWindDirection

def test_stddev_winddir_within_valid_range():
    stddev_winddir_obj = StdDevWindDirection(70, "DEGREES")
    assert stddev_winddir_obj.is_valid() == True

def test_stddev_winddir_outside_valid_range():
    stddev_winddir_obj = StdDevWindDirection(200, "DEGREES")
    assert stddev_winddir_obj.is_valid() == False

def test_stddev_winddir_invalid_creation():
    stddev_winddir_obj = StdDevWindDirection(None, "DEGREES")
    assert stddev_winddir_obj.wstdvM == -9999



    
