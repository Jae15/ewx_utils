class stdwinddirection:
    valid_wstdv_hourly_default = (0, 99)

    def __init__(self, wstdv, units, record_date=None):
        self.record_date = record_date
        if wstdv == None:
            wstdv = -9999
            unitsU = units.upper()
            if unitsU == "DEGREES":
                self.wstdvM = float(wstdv)

    def IsValid(self):
        if (
            self.wstdvM < self.valid_wstdv_hourly_default[0]
            and self.wstdvM > self.valid_wstdv_hourly_default[1]
        ):
            return True
        else:
            return False
