"""
Below are classes for soil moisture, soil heat flux and net radiation variables used to validate data retrieved from mawndb.
"""
         
class soilmoisture:
    valid_mstr_hourly_default = (0,1)
    def __init__(self, mstr, record_date = None):
        self.record_date = record_date
        self.mstr = mstr
    def IsValid(self):
        if self.mstr > self.valid_mstr_hourly_default[0] and self.mstr < self.valid_mstr_hourly_default[1]:
            return True
        else: 
            return False
        
class soilheatflux:
    valid_sflux_hourly_default = (0,7000)
    def __init__(self, sflux, record_date = None):
        self.record_date = record_date
        self.sflux = sflux
    def IsValid(self):
        if self.sflux > self.valid_sflux_hourly_default[0] and self.sflux < self.valid_sflux_hourly_default[1]:
            return True
        else:
            return False
        
class netradiation:
    valid_nrad_hourly_default = (-1250, 1250)
    def __init__(self, nrad, record_date = None):
        self.record_date = record_date
        self.nrad = nrad
    def IsValid(self):
        if self.nrad > self.valid_nrad_hourly_default[0] and self.nrad < self.valid_nrad_hourly_default[1]:
            return True
        else:
            return False
        
