class Mapper(object):
    def __init__(self, table_name=None):
        self.table_name = table_name
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
