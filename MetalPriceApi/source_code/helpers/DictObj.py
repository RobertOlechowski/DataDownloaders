import copy

from source_code.helpers.DumpBase import DumpBase


class DictObj(DumpBase):
    def __init__(self, d=None):
        if isinstance(d, DictObj):
            for k, v in d.__dict__.items():
                self.__dict__[k] = copy.deepcopy(v)
            return

        if isinstance(d, dict):
            self._build_dict(d)
            return

        raise Exception("Flow")

    def _build_dict(self, d):
        for a, b in d.items():
            if isinstance(b, (list, tuple)):
                setattr(self, a, [DictObj(x) if isinstance(x, dict) else x for x in b])
                continue
            setattr(self, a, DictObj(b) if isinstance(b, dict) else b)

    def get(self, name, default=None):
        return getattr(self, name, default)

    def get_path(self, name, default=None):
        path = name.split(".")
        _obj = self
        for item in path:
            _obj = getattr(_obj, item, None)
            if _obj is None:
                _obj = default
                break
        return _obj

    def get_not_none(self, name, default=None):
        return self.get(name, default) or default

    def keys(self):
        return self.__dict__.keys()

    def items(self):
        return self.__dict__.items()

    def clone(self):
        import copy
        return copy.deepcopy(self)
