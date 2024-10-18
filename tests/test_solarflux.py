from ewx_utils.mawndb_classes.soil_heat_flux import SoilHeatFlux

def test_sflux_within_valid_range_hourly_0():
    sflux_obj = SoilHeatFlux(0)
    assert sflux_obj.is_valid() is True

def test_sflux_within_valid_range_hourly_3500():
    sflux_obj = SoilHeatFlux(3500)
    assert sflux_obj.is_valid() is True

def test_sflux_within_valid_range_hourly_7000():
    sflux_obj = SoilHeatFlux(7000)
    assert sflux_obj.is_valid() is True

def test_sflux_below_valid_range():
    sflux_obj = SoilHeatFlux(-100)
    assert sflux_obj.is_valid() is False

def test_sflux_above_valid_range():
    sflux_obj = SoilHeatFlux(8000)
    assert sflux_obj.is_valid() is False

def test_sflux_edge_case_below_zero():
    sflux_obj = SoilHeatFlux(-1)
    assert sflux_obj.is_valid() is False

def test_sflux_edge_case_above_max():
    sflux_obj = SoilHeatFlux(7001)
    assert sflux_obj.is_valid() is False

def test_sflux_with_none_value():
    sflux_obj = SoilHeatFlux(None)
    assert sflux_obj.is_valid() is False