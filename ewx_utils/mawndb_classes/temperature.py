"""
The temperature class below defines the valid hourly default monthly ranges.
It also specifies the units of measurement and their respective conversions as stored in mawndb.
"""


class temperature:
    valid_hourly_atmp = {
        "jan": (-39, 25),
        "feb": (-39, 24),
        "mar": (-38, 33),
        "apr": (-29, 36),
        "may": (-13, 40),
        "jun": (-8, 44),
        "jul": (-5, 46),
        "aug": (-6, 44),
        "sep": (-13, 43),
        "oct": (-18, 37),
        "nov": (-33, 33),
        "dec": (-39, 25),
    }

    def __init__(self, temp, units, record_date=None):
        self.record_date = record_date
        if temp == None:
            self.tempC = None
            self.tempF = None
            self.tempK = None
            return
        unitsU = units.upper()
        nine_fifths = float(9) / float(5)
        if unitsU == "C":
            self.tempC = float(temp)
            self.tempF = nine_fifths * self.tempC + 32
            self.tempK = self.tempC + 273.15
        if unitsU == "F":
            self.tempF = float(temp)
            self.tempC = (1 / nine_fifths) * (self.tempF - 32)
            self.tempK = self.tempC + 273.15
        if unitsU == "K":
            self.tempK = float(temp)
            self.tempC = self.tempK - 273.15
            self.tempF = nine_fifths * self.tempC + 32

    def IsValid(self):
        if self == None:
            return False
        if self.tempC == None:
            return False
        if self.record_date is not None:
            datestring = self.record_date.strftime("%b %d, %Y")
            month_abbrv = (datestring[0:3]).lower()
            atmp_valid_range = self.valid_hourly_atmp[month_abbrv][0:2]
            return (
                atmp_valid_range[0] <= self.tempC and self.tempC <= atmp_valid_range[1]
            )
        if self.tempC < -40 or self.tempC > 46:
            return False
