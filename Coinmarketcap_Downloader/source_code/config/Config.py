import os

import yaml

from source_code.helpers.DictObj import DictObj
from source_code.helpers.DumpBase import DumpBase
from source_code.helpers.MinioWrapper import MinioWrapper
from source_code.helpers.other import getattr_ex, setattr_ex


def _config_constructor(loader, node):
    fields = loader.construct_mapping(node, deep=True)
    for item in fields.items():
        if isinstance(item[1], dict):
            fields[item[0]] = DictObj(item[1])
    return Config(**fields)


yaml.add_constructor('!Config', _config_constructor)


class Config(DumpBase):
    def __init__(self, **kwargs):
        self.db_engine = None
        self.mimo_wrapper = None

        for k in kwargs.keys():
            setattr(self, k, kwargs[k])

        _prefix = "COIN2DB_"
        extra_config = [(key[len(_prefix):].lower(), value) for key, value in os.environ.items() if key.startswith(_prefix)]

        for key, value in extra_config:
            _old_value = getattr_ex(self, key)
            if _old_value is not None and isinstance(_old_value, int):
                value = int(value)
            if _old_value is not None and isinstance(_old_value, float):
                value = float(value)
            if _old_value is not None and isinstance(_old_value, bool):
                value = bool(value)
            setattr_ex(self, key, value)

    def get_redis(self):
        import redis
        return redis.Redis(**self.redis.__dict__)

    def get_minio(self):
        if self.mimo_wrapper is None:
            from minio import Minio
            self.mimo_wrapper = MinioWrapper(Minio(**self.minio.__dict__))
        return self.mimo_wrapper

    def dump_config(self):
        print()
        print("========")
        self.dump()
        print("========")
        print()


class ConfigLoader:
    def __init__(self):
        pass

    def get_data(self):
        with open('config/config.yaml', 'r') as file:
            content = file.read()
        return yaml.load(content, Loader=yaml.FullLoader)
