import pickle
import time

from ROTools.Helpers.DictObj import DictObj

from source_code.helpers.BtcNode import BtcNode
from source_code.helpers.ProgresLog import ProgresLog
from source_code.workers.BaseWorker import BaseWorker, get_block_object_name


class Worker(BaseWorker):
    def __init__(self, index, stop_event):
        super().__init__(stop_event, "worker", index)

        self.redis = self.config.get_redis()
        self.node = BtcNode(self.config.nodes.btc)
        self.minio = self.config.get_minio()

    def init(self):
        time.sleep(self.local_config.delay_start)

    def _get_data(self, block_height):
        json_block = self.node.get_block_by_height(block_height=block_height)
        _block = DictObj(json_block)
        if _block.height != block_height:
            raise Exception("Invalid block height")
        return _block, json_block

    def step(self):
        data = self.redis.lpop("tasks")
        if data is None:
            self._wait_sleep()
            return

        coin_name, block_height = pickle.loads(data)
        if coin_name != "btc":
            raise Exception("Invalid coin")

        _block, json_block = self._get_data(block_height=block_height)
        object_name = get_block_object_name(block_height)
        self.minio.put_json(self.config.nodes.btc.bucket_name, object_name, json_block)

        self.redis.rpush("log", pickle.dumps(ProgresLog(name=self.name, height=_block.height)))
