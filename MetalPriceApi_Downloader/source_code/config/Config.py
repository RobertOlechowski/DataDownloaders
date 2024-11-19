import os

import yaml
from ROTools.Helpers.Attr import setattr_ex, getattr_ex
from ROTools.Helpers.DictObj import DictObj
from ROTools.Helpers.DumpBase import DumpBase
from ROTools.Wrappers.MinioWrapper import MinioWrapper


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

        _prefix = "METAL2DB_"
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
