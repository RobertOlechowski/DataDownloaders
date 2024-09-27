import os

import yaml
from sqlalchemy import create_engine, URL

from source_code.helpers.DictObj import DictObj
from source_code.helpers.DumpBase import DumpBase
from source_code.helpers.MinioWrapper import MinioWrapper


def getattr_ex(_obj, name):
    if isinstance(name, str):
        name = name.split(".")

    if isinstance(name, list):
        for item in name:
            if _obj is None:
                return None
            _obj = getattr(_obj, item, None)

        return _obj


def setattr_ex(_obj, name, value):
    if isinstance(name, str):
        name = name.split(".")

    if isinstance(name, list):
        for item in name[:-1]:
            _next = getattr(_obj, item, None)
            if _next is None:
                _next = DictObj({})
                setattr(_obj, item, _next)
            _obj = _next

        setattr(_obj, name[-1], value)


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

        _prefix = "BTC2DB_"
        extra_config = [(key[len(_prefix):].lower(), value) for key, value in os.environ.items() if key.startswith(_prefix)]

        for key, value in extra_config:
            _old_value = getattr_ex(self, key)
            if _old_value is not None and isinstance(_old_value, bool):
                setattr_ex(self, key, value in ["true", "True", "TRUE", "1", ])
                continue

            if _old_value is not None and isinstance(_old_value, int):
                setattr_ex(self, key, int(value))
                continue

            if _old_value is not None and isinstance(_old_value, float):
                setattr_ex(self, key, float(value))
                continue

            setattr_ex(self, key, value)

        if not self.mode.insert_db:
            self.app.inserter_count = 0

    def get_redis(self):
        import redis
        return redis.Redis(**self.redis.__dict__)

    def get_minio(self):
        if self.mimo_wrapper is None:
            from minio import Minio
            self.mimo_wrapper = MinioWrapper(Minio(**self.minio.__dict__))
        return self.mimo_wrapper

    def get_db_engine(self):
        if not self.mode.insert_db:
            return None
        if self.db_engine is not None:
            return self.db_engine
        database_url = URL.create(**self.db.__dict__)
        self.db_engine = create_engine(database_url)
        return self.db_engine

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
