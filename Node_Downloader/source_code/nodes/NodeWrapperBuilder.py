from source_code.nodes.btc.BtcNode import BtcNode


def BuildNodeWrappers( config):
    node_configs = [getattr(config.tasks, a) for a in config.run]
    _lut = {"btc": BtcNode}
    return [_lut[a.type](a) for a in node_configs]