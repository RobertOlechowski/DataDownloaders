import pickle

from source_code.helpers.DictObj import DictObj
from source_code.helpers.EthNode import EthNode
from source_code.helpers.ProgresLog import ProgresLog
from source_code.workers.BaseWorker import BaseWorker, get_block_object_name

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


class BlockDownloader(BaseWorker):
    def __init__(self, index, stop_event):
        super().__init__(stop_event, "block_downloader", index)

        self.redis = self.config.get_redis()
        self.node = EthNode(self.config)
        self.minio = self.config.get_minio()

    def init(self):
        pass


    def _get_data(self, block_height):
        object_name = get_block_object_name(block_height)

        json_block = self.node.get_block_by_height(block_height=block_height)

        for item in self.local_config.remove_elements:
            remove_from_dictionary(item, json_block)

        self.minio.put_json(self.config.app.cache_bucket, object_name, json_block)

        return DictObj(json_block)

    def step(self):
        if self.redis.llen("block_data") >= self.local_config.max_queue_size:
            self._wait_sleep()
            return

        data = self.redis.lpop("block_ids")
        if data is None:
            self._wait_sleep()
            return

        _block = self._get_data(block_height=int(data))
        _block_id = int(_block.number, 16)
        self.redis.rpush("log", pickle.dumps(ProgresLog(name=self.name, height=_block_id)))

