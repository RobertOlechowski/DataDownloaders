from source_code.helpers.EthNode import EthNode
from source_code.workers.BaseWorker import BaseWorker, get_block_object_name, bucket_name


def _find_highest(cb) -> int:
    low, high = 1, 15000000

    while low <= high:
        mid = (low + high) // 2
        if cb(mid):
            low = mid + 1
        else:
            high = mid - 1
    return high


class IdProducer(BaseWorker):
    def __init__(self, index, stop_event):
        super().__init__(stop_event, "id_producer", index)

        self.redis = self.config.get_redis()
        self.node = EthNode(self.config)
        self.minio = self.config.get_minio()

        self._start_range = None

    def get_top_block_height(self):
        _test_range = 200

        def is_present(numer) -> bool:
            object_name = get_block_object_name(numer)
            return self.minio.object_exists(bucket_name, object_name)

        _highest = _find_highest(is_present)
        _min_range = max(0, _highest - _test_range)
        result = [a for a in range(_min_range, _highest + 1) if is_present(a)]
        return set(result)

    def init(self):
        _ids_in_db = self.get_top_block_height()

        if len(_ids_in_db) == 0:
            self._start_range = 0
            print(f"Start range is: {self._start_range}")
            return

        _max = max(_ids_in_db)
        _ids_expected = set(range(min(_ids_in_db), _max + 1))

        _ids_missing = _ids_expected - _ids_in_db
        if len(_ids_missing) > 0:
            self.redis.rpush("block_ids", *_ids_missing)

        self._start_range = _max + 1
        print(f"Start range is: {self._start_range}")

    def step(self):
        max_queue_size = self.local_config.max_queue_size

        queue_size = self.redis.llen("block_ids")
        batch_size = max_queue_size - queue_size + 1

        _max_id = self.node.get_latest_block_number()
        step = 1
        _end_range = min(self._start_range + (batch_size * step), _max_id)

        elements = list(range(self._start_range, _end_range, step))

        if queue_size >= (0.8 * max_queue_size) or len(elements) == 0:
            self._wait_sleep()
            return

        self.redis.rpush("block_ids", *elements)
        self._start_range = max(elements) + 1

