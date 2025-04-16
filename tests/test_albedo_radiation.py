from ewx_utils.mawndb_classes.albedo_radiation import Albedo
from datetime import datetime

# Tests for values within valid range
def test_albedo_zero():
    """Test albedo value at lower boundary (0)"""
    albedo_obj = Albedo(0)
    assert albedo_obj.is_valid() is True

def test_albedo_one():
    """Test albedo value at upper boundary (1)"""
    albedo_obj = Albedo(1)
    assert albedo_obj.is_valid() is True

def test_albedo_middle_value():
    """Test albedo value in middle of valid range"""
    albedo_obj = Albedo(0.5)
    assert albedo_obj.is_valid() is True

def test_albedo_low_valid():
    """Test albedo value near lower boundary"""
    albedo_obj = Albedo(0.01)
    assert albedo_obj.is_valid() is True

def test_albedo_high_valid():
    """Test albedo value near upper boundary"""
    albedo_obj = Albedo(0.99)
    assert albedo_obj.is_valid() is True

