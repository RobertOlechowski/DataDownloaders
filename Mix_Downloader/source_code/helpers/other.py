def is_equal(a, b, epsilon=1e-6):
    return abs(a - b) < epsilon


def group_list(data, cb):
    from collections import defaultdict
    db = defaultdict(list)
    for item in data:
        k = cb(item)
        db[k].append(item)
    return db


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def parse_int(text):
    try:
        return int(text)
    except:
        return None


def parse_float(text, error_value=None):
    try:
        return float(text)
    except:
        return error_value