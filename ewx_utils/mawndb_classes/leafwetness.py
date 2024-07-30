"""
Below is the precipitation class validation code which defines the valid fivemin, hourly and daily default monthly ranges.
The units of measurements as stored in mawndb and their respective conversions, sensors and tables are also specified.
"""


class leafwetness:
    def __init__(self, lw, table, sensor, record_date=None):
        self.record_date = record_date
        if lw == None:
            lw = -9999  # what would it take to keep this as None?
        self.tableU = table.upper()
        self.lw = lw
        if isinstance(self.lw, (int, float)):
            self.percent = str(int(self.lw * 100 + 0.5))  # only makes sense for hourly.
        else:
            self.percent = -9999
        self.sensorU = sensor.upper()
        # if tableU = 'FIVEMIN':
        # (valid if > 0), wet if < 5000
        # NEED TO DEAL WITH HOURLY ALSO.

    def IsValid(self):
        if self.tableU == "FIVEMIN":
            if self.sensorU == "LEAF0" or self.sensorU == "LEAF1":
                if self.lw < -100 or self.lw >= 9999:
                    return False
                else:
                    return True
            else:  # lws0 or lws1
                return True  # all values valid/no valid range.

        if self.tableU == "HOURLY":
            if self.lw < 0 or self.lw > 1:
                return False
            else:
                return True

    def IsWet(self):
        if self.tableU == "FIVEMIN":
            if self.sensorU == "LEAF0" or self.sensorU == "LEAF1":
                if self.IsValid and self.lw < 9999 and self.lw >= -100:
                    return True
                else:
                    return False
            else:  # lws0 or lws1
                if self.IsValid and self.lw >= 280:
                    return True
                else:
                    return False
        if self.tableU == "HOURLY":
            if self.IsValid and self.lw >= 0.25:
                return True
            else:
                return False
