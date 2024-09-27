class Mapper(object):
    def __init__(self):
        self.map_list = []

    def add(self, map_item):
        self.map_list.append(map_item)

    def add_mapper(self, name, _mapper):
        new_mappers = _mapper.map_list[:]
        for item in new_mappers:
            item.name = f"{name}.{item.name}"
        self.map_list.extend(new_mappers)

    def from_json(self, class_type, data, ex_data=None):
        obj = class_type()
        for item in [a for a in self.map_list if a.mapper_type in ["json_map", "const_map"]]:
            item.set_from_json(obj, data, ex_data=ex_data)
        return obj


def check_fields(data, length, fields, ignore=()):
    fields_set = set(data.__dict__)
    fields_set.difference_update(ignore)
    if len(fields_set) not in length:
        print(fields_set)
        raise Exception(f"Input data must have exactly {length} keys, but has {len(fields_set)}")

    for item in fields:
        if item not in data.__dict__:
            raise Exception(f"Field [{item}] is missing")


def _remove_from_dictionary(key_path, obj):
    k = key_path.pop(0)

    element = obj.get(k, None)
    if element is None:
        return

    if len(key_path) == 0:
        obj.pop(k)
        return

    if isinstance(element, list):
        for item in element:
            _remove_from_dictionary(key_path[:], item)
        return

    if isinstance(element, dict):
        _remove_from_dictionary(key_path[:], element)
        return

    raise Exception("Flow")


def remove_from_dictionary(key, dictionary):
    key_path = key.split(".")
    _remove_from_dictionary(key_path, dictionary)
