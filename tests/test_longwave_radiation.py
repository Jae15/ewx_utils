from ewx_utils.mawndb_classes.longwave_radiation import LongwaveRadiation
from datetime import datetime

# Tests for incoming longwave radiation
def test_lwin_within_valid_range_lower_bound():
    lwin_obj = LongwaveRadiation("lwin", 100, summation="incoming")
    assert lwin_obj.is_valid() is True

def test_lwin_within_valid_range_middle():
    lwin_obj = LongwaveRadiation("lwin", 300, summation="incoming")
    assert lwin_obj.is_valid() is True

def test_lwin_within_valid_range_upper_bound():
    lwin_obj = LongwaveRadiation("lwin", 500, summation="incoming")
    assert lwin_obj.is_valid() is True

def test_lwin_below_valid_range():
    lwin_obj = LongwaveRadiation("lwin", 99, summation="incoming")
    assert lwin_obj.is_valid() is False

def test_lwin_above_valid_range():
    lwin_obj = LongwaveRadiation("lwin", 501, summation="incoming")
    assert lwin_obj.is_valid() is False

# Tests for outgoing longwave radiation
def test_lwout_within_valid_range():
    lwout_obj = LongwaveRadiation("lwout", 300, summation="outgoing")
    assert lwout_obj.is_valid() is True

def test_lwout_below_valid_range():
    lwout_obj = LongwaveRadiation("lwout", 99, summation="outgoing")
    assert lwout_obj.is_valid() is False

def test_lwout_above_valid_range():
    lwout_obj = LongwaveRadiation("lwout", 501, summation="outgoing")
    assert lwout_obj.is_valid() is False

