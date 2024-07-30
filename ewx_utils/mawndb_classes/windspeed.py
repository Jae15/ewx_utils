"""
The windspeed class below defines the valid hourly default monthly ranges.
It also specifies the units of measurement and their respective conversions as stored in mawndb.
"""


class windspeed:
    valid_wspd_hourly_default = (0, 99)

    def __init__(self, wspd, units, record_date=None):
        self.record_date = record_date
        if wspd == None:
            wspd = -9999
        unitsU = units.upper()
        if unitsU == "MPS":  # meters per second
            self.wspdMPS = float(wspd)
            self.wspdMPH = float(wspd) / 1609.344 * 3600
            # 1609 meters per mile
            # 3600 seconds per hour
        if unitsU == "MPH":  # miles per hour
            self.wspdMPS = float(wspd) * 1609.344 / 3600
            self.wspdMPH = float(wspd)

    def IsValid(self):
        if (
            self.wspdMPS > self.valid_wspd_hourly_default[0]
            and self.wspdMPS < self.valid_wspd_hourly_default[1]
        ):
            return True
        else:
            return False
