from source_code.nodes.btc.BtcNode import BtcNode
from source_code.nodes.eth.EthNode import EthNode


def BuildNodeWrappers( config):
    node_configs = [getattr(config.tasks, a) for a in config.run]
    _lut = {"btc": BtcNode, "eth": EthNode}
    return [_lut[a.type](a) for a in node_configs]