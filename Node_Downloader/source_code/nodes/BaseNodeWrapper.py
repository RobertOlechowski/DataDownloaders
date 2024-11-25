from datetime import datetime, timezone

from source_code.workers.BaseWorker import get_block_object_name

def _find_highest(cb) -> int:
    low, high = 1, int(1e9)

    while low <= high:
        mid = (low + high) // 2
        if cb(mid):
            low = mid + 1
        else:
            high = mid - 1
    return high

class BaseNodeWrapper:
    def __init__(self, node_config):
        self.config = node_config
        self.type = node_config.type

        self.generator_position = None
        self.max_block_height = -1
        self.delta_t = None
        self.blocs_per_hour = None
        self.logs_count = None

        self.latest_block_number = None
        self.refresh_block_time = datetime.fromisoformat('1900-01-01')

    def get_latest_block_number(self):
        now = datetime.now()
        days_seconds = (now - self.refresh_block_time).seconds
        if days_seconds > 60:
            self.latest_block_number = self.node.get_latest_block_number()
            self.refresh_block_time = now

        return self.latest_block_number

    def gen_ids_batch(self, batch_size):
        _max_available_block = self.get_latest_block_number()
        _end_range = min(self.generator_position + batch_size, _max_available_block)
        elements = list(range(self.generator_position, _end_range + 1))
        if len(elements) > 0:
            self.generator_position = max(elements) + 1
        return elements

    def gen_missing_ids(self, minio=None, test_range=None):
        _ids_in_db = self._get_top_block_height(minio=minio, test_range=test_range)

        if len(_ids_in_db) == 0:
            self.generator_position = 0
            print(f"Start range for [{self.type}] is: {self.generator_position}")
            return []

        _max = max(_ids_in_db)

        _ids_expected = set(range(min(_ids_in_db), _max + 1))
        _ids_missing = _ids_expected - _ids_in_db

        self.generator_position = _max + 1
        print(f"Start range for [{self.type}] is: {self.generator_position}")

        return _ids_missing


    def _get_top_block_height(self, minio=None, test_range=None):
        def is_present(numer) -> bool:
            object_name = get_block_object_name(block_height=numer, coin_name=self.type)
            return minio.object_exists(self.config.bucket_name, object_name)

        _highest = _find_highest(is_present)
        _min_range = max(0, _highest - test_range)
        result = [a for a in range(_min_range, _highest + 1) if is_present(a)]
        return set(result)

    def process_logs(self, logs):
        self.delta_t = None
        self.blocs_per_hour = None

        logs = [a for a in logs if a.node_type == self.type]
        values = [a.height for a in logs]
        self.max_block_height = max([self.max_block_height, *values])

        self.logs_count = len(logs)

        if self.logs_count > 1:
            self.delta_t = logs[-1].get_time_delta(logs[0])
            self.blocs_per_hour = 60.0 * 60.0 * float(self.logs_count) / self.delta_t