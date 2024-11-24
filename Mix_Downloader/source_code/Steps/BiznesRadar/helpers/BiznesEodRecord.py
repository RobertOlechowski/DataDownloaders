from datetime import datetime

from ROTools.Helpers.DictObj import DictObj


def _parse_int(value):
    if value is None or value == '':
        return None
    return int(value.replace(" ", ""))

class BiznesEodRecord:
    def __init__(self, data, symbol):
        if isinstance(data, tuple):
            data = list(data)

            if symbol.type in ["Index"]:
                data.append(None)

            if len(data) != 7:
                raise ValueError("Data error")

            _data, _open, _max, _min, _close, volume, turnover = data

            self.time = datetime.strptime(_data, '%d.%m.%Y').date()
            self.open = float(_open)
            self.close = float(_close)
            self.low = float(_min)
            self.high = float(_max)
            self.volume = _parse_int(volume)
            self.turnover = _parse_int(turnover)

        if isinstance(data, DictObj):
            self.time = datetime.fromisoformat(data.time).date()
            self.open = data.open
            self.close = data.close
            self.low = data.low
            self.high = data.high

            if hasattr(data, "volume"):
                self.volume = data.volume

            if hasattr(data, "turnover"):
                self.turnover = data.turnover

    def to_dict(self):
        obj = dict(time=self.time.isoformat(), open=self.open, close=self.close, low=self.low, high=self.high)
        if self.volume is not None:
            obj["volume"] = self.volume
        if self.turnover is not None:
            obj["turnover"] = self.turnover
        return obj
