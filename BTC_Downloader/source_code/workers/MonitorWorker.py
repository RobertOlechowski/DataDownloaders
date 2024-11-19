import pickle

from ROTools.Helpers.Info import print_info
from ROTools.Helpers.RedisSingletonLock import RedisSingletonLock
from source_code.helpers.BtcNode import BtcNode


class MonitorWorker(object):
    def __init__(self, config):
        self.config = config
        self.redis = self.config.get_redis()
        self.app_lock = RedisSingletonLock(self.redis, config.app.lock_timeout)
        self.node = BtcNode(self.config)
        self.max_block_height = None

    def init(self):
        self.app_lock.acquire_lock()

        print_info()

        if self.config.app.config_dump:
            self.config.dump_config()

    def monitor_cb(self):
        self.app_lock.refresh_lock()

        config = self.config

        log_records = [pickle.loads(a) for a in self.redis.lrange("log", 0, -1)]
        old_logs = [a for a in log_records if a.is_older_than(config.app.monitor_observation)]

        to_delete = len(old_logs)
        self.redis.lpop("log", count=to_delete)

        all_elements = [pickle.loads(a) for a in self.redis.lrange("log", 0, -1)]

        _lq1 = self.redis.llen("block_ids")
        _w2 = config.app.downloader_count

        _lq_log = len(all_elements)

        q_len_text = f"[{_lq1:>4}] worker: [{_w2:>2}]   log: [{_lq_log:>4}]"

        progress_text = "???"

        latest_block_number = self.node.get_latest_block_number()

        if len(all_elements) > 0:
            self.max_block_height = max(self.max_block_height or -1, max([a.height for a in all_elements]))

        if self.max_block_height is not None:
            progress = 100.0 * self.max_block_height / latest_block_number
            blocks_left = latest_block_number - self.max_block_height
            last_block_height_text = f"{self.max_block_height:>8,}".replace(',', " ")
            blocks_left_text = f"{blocks_left:>8,}".replace(',', " ")
            progress_text = f"max_downloaded: {last_block_height_text}  left: {blocks_left_text}  progress: {progress:>5.2f}%"

        print(f"===\t{q_len_text}\t|\t {progress_text}", flush=True)

        if len(all_elements) > 1:
            duration = all_elements[-1].get_time_delta(all_elements[0])
            blocs_per_hour = 60.0 * 60.0 * float(_lq_log) / duration
            progress_per_hour = (blocs_per_hour / latest_block_number) * 100.0
            blocs_per_min = blocs_per_hour / 60.0

            to_process_count = latest_block_number - self.max_block_height
            eta = (to_process_count / blocs_per_hour) / 24.0

            _speed_text = f"speed: {blocs_per_min:>4.0f} b/min   {progress_per_hour:>4.2f} %/h  eta: {eta:3.1f} days"
            print(f"\t[t:{duration:.1f} del:{to_delete:>3} ] {_speed_text}", flush=True)
        print()
