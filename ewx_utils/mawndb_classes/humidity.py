"""
The humidity class below defines the valid hourly default monthly ranges.
It also specifies the units of measurement and their respective conversions as stored in mawndb.
"""
class humidity:
    valid_relh_hourly_default = (0,100)
    RELH_CAP = 105
    def __init__(self, relh, units, record_date = None):
        self.record_date = record_date
        if relh == None:
            relh = -9999
        unitsU = units.upper()
        if unitsU == "PCT":
            self.relhPCT = float(relh)
            self.relhDEC = float(relh) / 100
        if unitsU == "DEC":
            self.relhPCT = float(relh) * 100
            self.relhDEC = float(relh)
    def IsValid(self):
        if self.relhPCT > self.valid_relh_hourly_default[0] and self.relhPCT  < self.valid_relh_hourly_default[1]:
            return True
        else:
            return False
    def IsInRange(self):
        if self.relhPCT >= self.valid_relh_hourly_default[1] and self.relhPCT <= self.RELH_CAP:
            return (100, "RELH_CAP")
        else:
            return (None, None)
