"""
The precipitation class below defines the valid hourly default monthly ranges.
It also specifies the units of measurement and their respective conversions and tables as stored in mawndb.
"""      
class precipitation:
    valid_pcpn_fivemin_default = (0,14)
    valid_pcpn_hourly_default = (0,77)
    valid_pcpn_daily_default = (0,254)
    def __init__(self, pcpn, table: str, units, record_date = None):
        self.record_date = record_date
        self.src = None
        self.tableU = table.upper()
        if pcpn == None:
            pcpn = -9999
        unitsU = units.upper()
        if unitsU == "MM":
            self.pcpnMM = float(pcpn)
            self.pcpnIN = float(pcpn) / 25.4
        if unitsU == "IN":
            self.pcpnMM = float(pcpn) * 25.4
            self.pcpnIN = float(pcpn)
    def set_src(self, src):
        self.src = src
    def IsValid(self):
        if self.tableU == "FIVEMIN":
            validation_range = self.valid_pcpn_fivemin_default
        elif self.tableU == "HOURLY":
            validation_range = self.valid_pcpn_hourly_default
        elif self.tableU == "DAILY":
            validation_range = self.valid_pcpn_daily_default
        if self.pcpnMM > validation_range[0] and self.pcpnMM < validation_range[1]:
            return True
        else:
            return False
        
