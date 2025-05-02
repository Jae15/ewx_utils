from ewx_utils.mawndb_classes.shortwave_radiation import ShortwaveRadiation

def test_swin_within_valid_range_negative():
    swin_obj = ShortwaveRadiation("swin", -1, summation="incoming")
    assert swin_obj.is_valid() is True

def test_swin_within_valid_range_zero():
    swin_obj = ShortwaveRadiation("swin", 0, summation="incoming")
    assert swin_obj.is_valid() is True

def test_swin_within_valid_range_positive():
    swin_obj = ShortwaveRadiation("swin", 1000, summation="incoming")
    assert swin_obj.is_valid() is True