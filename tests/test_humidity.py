from ewx_utils.mawndb_classes.humidity import Humidity

def test_humidity_conversion_pct_to_dec():
    hum_pct = Humidity(50, "PCT")
    assert hum_pct.relhPCT == 50, "Expected relhPCT to be 50"
    assert hum_pct.relhDEC == 0.5, "Expected relhDEC to be 0.5"

def test_humidity_conversion_dec_to_pct():
    hum_dec = Humidity(0.75, "DEC")  
    assert hum_dec.relhPCT == 75, "Expected relhPCT to be 75"
    assert hum_dec.relhDEC == 0.75, "Expected relhDEC to be 0.75"

def test_humidity_conversion_edge_cases():
    # Test case for edge case at 0%
    hum_zero_pct = Humidity(0, "PCT")
    assert hum_zero_pct.relhPCT == 0, "Expected relhPCT to be 0"
    assert hum_zero_pct.relhDEC == 0.0, "Expected relhDEC to be 0.0"

    # Test case for edge case at 100%
    hum_hundred_pct = Humidity(100, "PCT")
    assert hum_hundred_pct.relhPCT == 100, "Expected relhPCT to be 100"
    assert hum_hundred_pct.relhDEC == 1.0, "Expected relhDEC to be 1.0"

def test_humidity_init_valid():
    hum = Humidity(50, "PCT")
    assert hum.relhPCT == 50, "Expected relhPCT to be 50"
    assert hum.relhDEC == 0.5, "Expected relhDEC to be 0.5"

def test_humidity_init_minimum():
    hum_min = Humidity(0, "PCT")
    assert hum_min.relhPCT == 0, "Expected relhPCT to be 0"
    assert hum_min.relhDEC == 0.0, "Expected relhDEC to be 0.0"

def test_humidity_init_maximum():
    hum_max = Humidity(100, "PCT")
    assert hum_max.relhPCT == 100, "Expected relhPCT to be 100"
    assert hum_max.relhDEC == 1.0, "Expected relhDEC to be 1.0"

def test_humidity_is_valid_below_range():
    Humidity.valid_relh_hourly_default = (0, 100)
    hum_below = Humidity(-10, "PCT")
    assert hum_below.is_valid() == False, "Expected False for humidity value -10"

def test_humidity_is_valid_at_lower_boundary():
    Humidity.valid_relh_hourly_default = (0, 100)
    hum_at_lower = Humidity(0, "PCT")
    assert hum_at_lower.is_valid() == True, "Expected True for humidity value 0"

def test_humidity_is_valid_within_range():
    Humidity.valid_relh_hourly_default = (0, 100)
    hum_within = Humidity(50, "PCT")
    assert hum_within.is_valid() == True, "Expected True for humidity value 50"

def test_humidity_is_valid_at_upper_boundary():
    Humidity.valid_relh_hourly_default = (0, 100)
    hum_at_upper = Humidity(100, "PCT")
    assert hum_at_upper.is_valid() == True, "Expected True for humidity value 100"

def test_humidity_is_valid_above_range():
    Humidity.valid_relh_hourly_default = (0, 100)
    hum_above = Humidity(110, "PCT")
    assert hum_above.is_valid() == False, "Expected False for humidity value 110"

def test_hum_in_range_above_max():
    hum_high = Humidity(106, "PCT")
    assert hum_high.is_in_range() is None, "Expected None for humidity above 105%"

def test_hum_in_range_at_cap():
    hum_cap = Humidity(105, "PCT")
    assert hum_cap.is_in_range() == (100, "RELH_CAP"), "Expected to be capped at 100%"

def test_hum_in_range_just_below_cap():
    hum_just_below_cap = Humidity(104, "PCT")
    assert hum_just_below_cap.is_in_range() == (100, "RELH_CAP"), "Expected to be capped at 100%"

def test_hum_in_range_within_valid_range():
    hum_valid = Humidity(50, "PCT")
    assert hum_valid.is_in_range() == (50, "RELH"), "Expected to be 50% relative humidity"
