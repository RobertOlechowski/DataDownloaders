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