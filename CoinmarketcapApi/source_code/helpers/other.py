from source_code.helpers.DictObj import DictObj


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
