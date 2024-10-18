from ewx_utils.mawndb_classes.net_radiation import NetRadiation

def test_nrad_within_valid_range_hourly_negative_1250():
    nrad_obj = NetRadiation(-1250)
    assert nrad_obj.is_valid() is True

def test_nrad_within_valid_range_hourly_0():
    nrad_obj = NetRadiation(0)
    assert nrad_obj.is_valid() is True

def test_nrad_within_valid_range_hourly_positive_1250():
    nrad_obj = NetRadiation(1250)
    assert nrad_obj.is_valid() is True

def test_nrad_below_valid_range():
    nrad_obj = NetRadiation(-1300)
    assert nrad_obj.is_valid() is False

def test_nrad_above_valid_range():
    nrad_obj = NetRadiation(1300)
    assert nrad_obj.is_valid() is False

def test_nrad_edge_case_below_min():
    nrad_obj = NetRadiation(-1251)
    assert nrad_obj.is_valid() is False

def test_nrad_edge_case_above_max():
    nrad_obj = NetRadiation(1251)
    assert nrad_obj.is_valid() is False

def test_nrad_with_none_value():
    nrad_obj = NetRadiation(None)
    assert nrad_obj.is_valid() is False
