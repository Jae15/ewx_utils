""" 
The evapotranspiration class below defines the valid hourly default monthly ranges.
It also specifies the units of measurement and their respective conversions as stored in mawndb.
"""


class evapotranspiration:
    valid_rpet_hourly_default = (0, 10)
    valid_rpet_daily_default = (0, 10)

    def __init__(self, rpet, table: str, units, record_date=None):
        self.record_date = record_date
        self.src = None
        self.tableU = table.upper()
        if rpet == None:
            rpet = -9999
        unitsU = units.upper()
        if unitsU == "MM":
            self.rpetMM = float(rpet)
            self.rpetIN = float(rpet) / 25.4
        if unitsU == "IN":
            self.rpetMM = float(rpet) * 25.4
            self.rpetIN = float(rpet)

    def set_src(self, src):
        self.src = src

    def IsValid(self):
        if self.tableU == "HOURLY":
            validation_range = self.valid_rpet_hourly_default
        elif self.tableU == "DAILY":
            validation_range = self.valid_rpet_daily_default
        if self.rpetMM > validation_range[0] and self.rpetMM < validation_range[1]:
            return True
        else:
            return False
