
import pickle

from source_code.nodes.NodeWrapperBuilder import BuildNodeWrappers
from source_code.workers.BaseWorker import BaseWorker


class IdProducer(BaseWorker):
    def __init__(self, index, stop_event):
        super().__init__(stop_event, "id_producer", index)

        self.redis = self.config.get_redis()
        self.minio = self.config.get_minio()
        self.node_wrappers = BuildNodeWrappers(self.config)

    def init(self):
        for item in self.node_wrappers:
            _ids_missing = item.gen_missing_ids(minio=self.minio, test_range=500)
            if len(_ids_missing) > 0:
                elements = [(item.type, a) for a in _ids_missing]
                self.redis.rpush("block_ids", *[pickle.dumps(a) for a in elements])


    def _gen_ids_mini(self, mini_batch_size):
        result = []
        for item in self.node_wrappers:
                elements = item.gen_ids_batch(batch_size=mini_batch_size)
                result.extend([(item.type, a) for a in elements])
        return result

    def step(self):
        max_queue_size = self.config.app.max_queue_size
        queue_size = self.redis.llen("tasks")
        batch_size = max_queue_size - queue_size + 1
        if queue_size >= (0.8 * max_queue_size):
            self._wait_sleep()
            return

        elements = []
        while True:
            new_elements = self._gen_ids_mini(mini_batch_size=10)
            elements.extend(new_elements)
            if len(new_elements) == 0 or len(elements) > batch_size:
                break

        if len(elements) == 0:
            self._wait_sleep()
            return

        self.redis.rpush("tasks", *[pickle.dumps(a) for a in elements])

