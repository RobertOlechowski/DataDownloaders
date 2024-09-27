import pickle

from source_code.DAO.EthBlockDAO import EthBlockDAO
from source_code.Mapper.Mapper import remove_from_dictionary
from source_code.helpers.DictObj import DictObj
from source_code.helpers.EthNode import EthNode
from source_code.helpers.ProgresLog import ProgresLog
from source_code.workers.BaseWorker import BaseWorker, bucket_name, get_block_object_name


class BlockDownloader(BaseWorker):
    def __init__(self, index, stop_event):
        super().__init__(stop_event, "block_downloader", index)

        self.redis = self.config.get_redis()
        self.node = EthNode(self.config)
        self.minio = self.config.get_minio()

    def init(self):
        pass

    def _get_cache_data(self, object_name):
        if not self.config.mode.read_cache:
            return None
        return self.minio.get_json(bucket_name, object_name)

    def _get_data(self, block_height):
        object_name = get_block_object_name(block_height)

        cached_json_block = self._get_cache_data(object_name)
        json_block = cached_json_block or self.node.get_block_by_height(block_height=block_height)

        for item in self.local_config.remove_elements:
            remove_from_dictionary(item, json_block)

        if self.config.mode.save_cache and cached_json_block is None:
            self.minio.put_json(bucket_name, object_name, json_block)

        _block = EthBlockDAO.FromJson(DictObj(json_block))
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
            self.redis.rpush("log", pickle.dumps(ProgresLog(name=self.name, height=_block.id)))

