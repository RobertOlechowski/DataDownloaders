import pickle
import time

from ROTools.Helpers.DictObj import DictObj

from source_code.helpers.Other import remove_from_dictionary
from source_code.nodes.BaseNodeWrapper import BaseNodeWrapper
from source_code.nodes.NodeWrapperBuilder import BuildNodeWrappers
from source_code.nodes.btc.BtcRequestWrapper import BtcRequestWrapper
from source_code.helpers.ProgresLog import ProgresLog
from source_code.workers.BaseWorker import BaseWorker, get_block_object_name


_lut_block_height = {"btc": lambda a: a.height, "eth": lambda a: int(a.number, 16)}

class Worker(BaseWorker):
    def __init__(self, index, stop_event):
        super().__init__(stop_event, "worker", index)

        self.redis = self.config.get_redis()
        self.minio = self.config.get_minio()

        self.node_wrappers = BuildNodeWrappers(self.config)
        self.node_wrappers = {a.type: a for a in self.node_wrappers}

    def init(self):
        time.sleep(self.config.app.worker_delay_start)

    def _get_data(self, node_wrapper, block_height_requested):
        json_block = node_wrapper.node.get_block_by_height(block_height=block_height_requested)
        _block = DictObj(json_block)
        return _block, json_block

    def step(self):
        data = self.redis.lpop("tasks")
        if data is None:
            self._wait_sleep()
            return

        coin_name, block_height_requested = pickle.loads(data)
        _node_wrapper = self.node_wrappers[coin_name]

        _block, json_block = self._get_data(node_wrapper=_node_wrapper, block_height_requested=block_height_requested)
        block_height = _lut_block_height[coin_name](_block)

        if block_height != block_height_requested:
            raise Exception("Invalid block height")

        if hasattr(_node_wrapper.config, "remove_elements"):
            for item in _node_wrapper.config.remove_elements:
                remove_from_dictionary(item, json_block)


        object_name = get_block_object_name(coin_name=coin_name, block_height=block_height_requested)
        self.minio.put_json(_node_wrapper.config.bucket_name, object_name, json_block)

        self.redis.rpush("log", pickle.dumps(ProgresLog(node_type=coin_name, height=block_height)))
