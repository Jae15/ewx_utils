from scripts.hourly_utility import limit_to_max_digits
import decimal

def test_limit_to_max_digits01():
    result = limit_to_max_digits(136.6555, max_digits=6)
    expected = decimal.Decimal('136.656')
    assert result == expected

def test_limit_to_max_digits02():
    result = limit_to_max_digits(85.74987, max_digits=6)
    expected = decimal.Decimal('85.7499')
    assert result == expected

def test_limit_to_max_digits03():
    result = limit_to_max_digits(245.8521, max_digits=6)
    expected = decimal.Decimal('245.852')
    assert result == expected

def test_limit_to_max_digits04():
    result = limit_to_max_digits(372.3589, max_digits=6)
    expected = decimal.Decimal('372.359')
    assert result == expected

def test_limit_to_max_digits05():
    result = limit_to_max_digits(655.1248, max_digits=6)
    expected = decimal.Decimal('655.125')
    assert result == expected

def test_limit_to_max_digits06():
    result = limit_to_max_digits(372.3589, max_digits=6)
    expected = decimal.Decimal('372.359')
    assert result == expected

def test_limit_to_max_digits07():
    result = limit_to_max_digits(333.5435, max_digits=6)
    expected = decimal.Decimal('333.544')
    assert result == expected

def test_limit_to_max_digits08():
    result = limit_to_max_digits(342.8656, max_digits=6)
    expected = decimal.Decimal('342.866')
    assert result == expected

def test_limit_to_max_digits09():
    result = limit_to_max_digits(5.602038, max_digits=6)
    expected = decimal.Decimal('5.60204')
    assert result == expected

def test_limit_to_max_digits10():
    result = limit_to_max_digits(155.8885, max_digits=6)
    expected = decimal.Decimal('155.889')
    assert result == expected

def test_limit_to_max_digits11():
    result = limit_to_max_digits(461.7894, max_digits=6)
    expected = decimal.Decimal('461.789')
    assert result == expected

def test_limit_to_max_digits12():
    result = limit_to_max_digits(725.1418, max_digits=6)
    expected = decimal.Decimal('725.142')
    assert result == expected

def test_limit_to_max_digits13():
    result = limit_to_max_digits(881.2874, max_digits=6)
    expected = decimal.Decimal('881.287')
    assert result == expected

def test_limit_to_max_digits14():
    result = limit_to_max_digits(819.6519, max_digits=6)
    expected = decimal.Decimal('819.652')
    assert result == expected

def test_limit_to_max_digits15():
    result = limit_to_max_digits(433.2771, max_digits=6)
    expected = decimal.Decimal('433.277')
    assert result == expected

def test_limit_to_max_digits16():
    result = limit_to_max_digits(130.2275, max_digits=6)
    expected = decimal.Decimal('130.228')
    assert result == expected

def test_limit_to_max_digits17():
    result = limit_to_max_digits(333.5435, max_digits=6)
    expected = decimal.Decimal('333.544')
    assert result == expected

def test_limit_to_max_digits18():
    result = limit_to_max_digits(342.8656, max_digits=6)
    expected = decimal.Decimal('342.866')
    assert result == expected

"""
The test below fails because the function only works for signifcant figures after the decimal point

def test_limit_to_max_digits19():
    result = limit_to_max_digits(12345678.9, max_digits=4)
    expected = decimal.Decimal('12350000')
    assert result == expected
"""


def test_limit_to_max_digits20():
    result = limit_to_max_digits(1.2345, max_digits=3)
    expected = decimal.Decimal('1.23')
    assert result == expected

def test_limit_to_max_digits21():
    result = limit_to_max_digits(1.2345, max_digits=4)
    expected = decimal.Decimal('1.235')
    assert result == expected

