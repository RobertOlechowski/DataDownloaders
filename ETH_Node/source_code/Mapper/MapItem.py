from datetime import datetime, timedelta
from enum import Enum

import dateutil
import pytz


class MapItem(object):
    def __init__(self, name, json_name=None, _type=None, none_exception=False, none_value=None):
        self.mapper_type = "json_map"
        self.name = name
        self.json_name = json_name or name
        self._type = _type
        self.none_exception = none_exception
        self.none_value = none_value

    def map_from_json(self, data):
        if data is None and self.none_exception:
            raise Exception(f"Value is None for [{self.name}]")

        if data is None:
            return self.none_value

        if self._type == object:
            return data

        if self._type == str:
            return data

        if self._type == list:
            return data

        if self._type == int:
            if isinstance(data, str) and len(data) == 0:
                return None
            return int(data)

        if self._type == float:
            return float(data)

        if self._type == bool:
            return bool(data)

        if self._type == "data_ns":
            return datetime(1970, 1, 1) + timedelta(microseconds=int(data / 1000))

        if self._type == "data_ms":
            return datetime(1970, 1, 1) + timedelta(milliseconds=data)

        if self._type == "data_s":
            return datetime(1970, 1, 1) + timedelta(seconds=data)

        if self._type == "data_ms_utc":
            return datetime(1970, 1, 1, tzinfo=pytz.utc) + timedelta(milliseconds=data)

        if self._type == "data_s_utc":
            return datetime(1970, 1, 1, tzinfo=pytz.utc) + timedelta(seconds=data)

        if self._type == "data_iso_tz":
            return dateutil.parser.isoparse(data)

        if self._type == "str_to_bytes":
            hex_string = data.replace("0x", "")
            if len(hex_string) % 2 != 0:
                hex_string = '0' + hex_string
            if len(hex_string) == 0:
                return None

            _val = bytes.fromhex(hex_string)
            return _val

        if self._type == "hex_to_bytes":
            import binascii
            binary_data = binascii.unhexlify(data)
            return binary_data

        if self._type == "hex_to_int":
            return int(data, 16)

        if self._type == "hex_to_int_time_utc":
            _val = int(data, 16)
            _val = datetime.utcfromtimestamp(_val)
            _val = pytz.UTC.localize(_val)
            return _val

        if self._type == Enum or self._type == "enum":
            if data in self.enum_values:
                return data
            raise Exception(f"Unknown Enum Value [{data}] [{self.name}]")

        if self._type == datetime:
            if type(data) == str:
                return dateutil.parser.isoparse(data).replace(tzinfo=None)
            if type(data) == datetime:
                return pytz.utc.localize(data)
            raise Exception(f"Value not in enum set [{data}]")

        if self._type == "datetime_utc":
            if type(data) == str:
                return dateutil.parser.isoparse(data).replace(tzinfo=pytz.utc)
            raise Exception(f"Value not in enum set [{data}]")

        raise Exception(f"Not implemented type {self._type}")

    def set_from_json(self, target_obj, data, ex_data):
        value_in = data.get_path(self.json_name)
        _value_out = self.map_from_json(value_in)
        setattr(target_obj, self.name, _value_out)
