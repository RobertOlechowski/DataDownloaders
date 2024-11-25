from source_code.nodes.BaseNodeWrapper import BaseNodeWrapper
from source_code.nodes.btc.BtcRequestWrapper import BtcRequestWrapper


class BtcNode(BaseNodeWrapper):
    def __init__(self, node_config):
        super().__init__(node_config)
        self.node = BtcRequestWrapper(node_config.node)








