import os
import pickle
from datetime import datetime, timezone

import humanize

from source_code.helpers.SingletonLock import SingletonLock


class MonitorWorker(object):
    def __init__(self, config):
        self.config = config
        self.redis = self.config.get_redis()
        self.app_lock = SingletonLock(self.redis, config.app.lock_timeout)
        self.minio = self.config.get_minio()

        self.total_logs = 0
        self.log_dict = {}

    def init(self):
        print()
        build_ver = os.getenv("RR_BUILD_VERSION")
        print(f"Build version    \t: {build_ver}")
        build_time = os.getenv("RR_BUILD_TIME")
        print(f"Build time UTC   \t: {build_time}")
        print(f"Current time UTC \t: {datetime.now(timezone.utc)}")
        if build_time is not None:
            build_time = datetime.fromisoformat(build_time)
            build_old = humanize.naturaltime(datetime.now(timezone.utc) - build_time)
            print(f"Build old\t\t: {build_old}")
        print()

        self.app_lock.acquire_lock()

        if self.config.app.config_dump:
            self.config.dump_config()

        _queue_names = ["tasks", "log"]
        for item in _queue_names:
            self.redis.delete(item)

    def _pop_all_logs(self):
        result = []
        while True:
            element = self.redis.lpop("log")
            if element is None:
                break
            result.append(pickle.loads(element))
        return result

    def _process_log(self, log):
        key = (log.name, log.sub_name)
        _old_log = self.log_dict.get(key, None)
        if _old_log is None:
            self.log_dict[key] = log
            return

        if _old_log.time < log.time:
            self.log_dict[key] = log
            log.tags = _old_log.tags
            return

    def monitor_cb(self):
        self.app_lock.refresh_lock()

        logs = self._pop_all_logs()
        _lq1 = self.redis.llen("tasks")

        for item in logs:
            self._process_log(item)

        _lq_log = len(logs)
        self.total_logs += len(logs)

        print(f"===\ttasks:[{_lq1:>4}]      log: [{_lq_log:>4}] [{self.total_logs:>4}]", flush=True)

        logs_sorted = list(self.log_dict.values())
        logs_sorted.sort(key=lambda a: a.time, reverse=False)

        counter = 0
        for item in logs_sorted:
            item.del_tag("to_show")
            item.del_tag("to_show_extra")

            if item.is_ended() and not item.check_tag("seen") and counter < self.config.app.log_count_extra:
                counter += 1
                item.add_tag("to_show_extra")

        for item in logs_sorted[-self.config.app.log_count:]:
            item.add_tag("to_show")

            if item.is_in_progress() or item.check_tag("to_show") or item.check_tag("to_show_extra"):
                item.add_tag("seen")
                print(item.get_log())

        print()
