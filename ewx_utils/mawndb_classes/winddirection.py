"""
The wind direction class below defines the valid hourly default monthly ranges.
It also specifies the units of measurement and their respective conversions and choice of degrees on a 0-360 scale as stored in mawndb.
"""


class winddirection:
    valid_wdir_hourly_default = (0, 360)

    def __init__(self, wdir, units, record_date=None):
        self.record_date = record_date
        if wdir == None:
            wdir = -9999
        unitsU = units.upper()
        if unitsU == "DEGREES":
            self.wdirDEGREES = float(wdir)
            x = self.wdirDEGREES
            choice = {
                x < 0: "NA",
                0 <= x < 11.25: "N",
                11.25 <= x < 33.75: "NNE",
                33.75 <= x < 56.25: "NE",
                56.25 <= x < 78.75: "ENE",
                78.75 <= x < 101.25: "E",
                101.25 <= x < 123.75: "ESE",
                123.75 <= x < 146.25: "SE",
                146.25 <= x < 168.75: "SSE",
                168.75 <= x < 191.25: "S",
                191.25 <= x < 213.75: "SSW",
                213.75 <= x < 236.25: "SW",
                236.25 <= x < 258.75: "WSW",
                258.75 <= x < 281.25: "W",
                281.25 <= x < 303.75: "WNW",
                303.75 <= x < 326.25: "NW",
                326.25 <= x < 348.75: "NNW",
                348.75 <= x <= 360: "N",
                x > 360: "NA",
            }
        self.wdirCOMPASS = choice[True]

    def IsValid(self):
        if (
            self.wdirDEGREES > self.valid_wdir_hourly_default[0]
            and self.wdirDEGREES < self.valid_wdir_hourly_default[1]
        ):
            return True
        else:
            return False
