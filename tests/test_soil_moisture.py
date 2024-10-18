from ewx_utils.mawndb_classes.soil_moisture import SoilMoisture

def test_mstr_within_valid_range_hourly_0():
    mstr_obj = SoilMoisture(0)
    assert mstr_obj.is_valid() is True

def test_mstr_within_valid_range_hourly_0_5():
    mstr_obj = SoilMoisture(0.5)
    assert mstr_obj.is_valid() is True

def test_mstr_within_valid_range_hourly_1():
    mstr_obj = SoilMoisture(1)
    assert mstr_obj.is_valid() is True

def test_mstr_below_valid_range():
    mstr_obj = SoilMoisture(-0.1)
    assert mstr_obj.is_valid() is False

def test_mstr_above_valid_range():
    mstr_obj = SoilMoisture(1.1)
    assert mstr_obj.is_valid() is False

def test_mstr_edge_case_below_zero():
    mstr_obj = SoilMoisture(-1)
    assert mstr_obj.is_valid() is False

def test_mstr_edge_case_above_one():
    mstr_obj = SoilMoisture(2)
    assert mstr_obj.is_valid() is False

def test_mstr_with_none_value():
    mstr_obj = SoilMoisture(None)
    assert mstr_obj.is_valid() is False

