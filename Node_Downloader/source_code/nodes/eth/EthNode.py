from source_code.nodes.BaseNodeWrapper import BaseNodeWrapper
from source_code.nodes.btc.BtcRequestWrapper import BtcRequestWrapper
from source_code.nodes.eth.EthRequestWrapper import EthRequestWrapper


class EthNode(BaseNodeWrapper):
    def __init__(self, node_config):
        super().__init__(node_config)
        self.node = EthRequestWrapper(node_config.node)









