import pickle
import time

from source_code.DAO.BtcBlockDAO import BtcBlockDAO
from source_code.helpers.BtcNode import BtcNode
from source_code.helpers.DictObj import DictObj
from source_code.helpers.ProgresLog import ProgresLog
from source_code.workers.BaseWorker import BaseWorker, bucket_name, get_block_object_name


class BlockDownloader(BaseWorker):
    def __init__(self, index, stop_event):
        super().__init__(stop_event, "block_downloader", index)

        self.redis = self.config.get_redis()
        self.node = BtcNode(self.config)
        self.minio = self.config.get_minio()

    def init(self):
        time.sleep(self.local_config.delay_start)

    def _get_cache_data(self, object_name):
        if not self.config.mode.read_cache:
            return None
        return self.minio.get_json(bucket_name, object_name)

    def _get_data(self, block_height):
        object_name = get_block_object_name(block_height)

        json_block = self._get_cache_data(object_name)
        save_to_cache = json_block is None
        if json_block is None:
            json_block = self.node.get_block_by_height(block_height=block_height)

        _block = BtcBlockDAO.FromJson(DictObj(json_block))
        if _block.height != block_height:
            raise Exception("Invalid block height")

        if save_to_cache and self.config.mode.save_cache:
            self.minio.put_json(bucket_name, object_name, json_block)

        return _block

    def step(self):
        if self.redis.llen("block_data") >= self.local_config.max_queue_size:
            self._wait_sleep()
            return

        data = self.redis.lpop("block_ids")
        if data is None:
            self._wait_sleep()
            return

        _block = self._get_data(block_height=int(data))

        if self.config.mode.insert_db:
            _block_serialized = pickle.dumps(_block)
            self.redis.rpush("block_data", _block_serialized)
        else:
            self.redis.rpush("log", pickle.dumps(ProgresLog(name=self.name, height=_block.height)))
