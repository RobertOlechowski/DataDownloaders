import pickle

from ROTools.Helpers.Info import print_info
from ROTools.Helpers.RedisSingletonLock import RedisSingletonLock

from source_code.nodes.NodeWrapperBuilder import BuildNodeWrappers


class MonitorWorker(object):
    def __init__(self, config):
        self.config = config
        self.redis = self.config.get_redis()
        self.minio = self.config.get_minio()

        self.app_lock = RedisSingletonLock(self.redis, config.app.lock_timeout)
        self.node_wrappers = BuildNodeWrappers(config)

    def init(self):
        self.app_lock.acquire_lock()

        print_info()

        if self.config.app.config_dump:
            self.config.dump_config()

        for item in self.node_wrappers:
            item.init_max_block_height(minio=self.minio)

    def _get_logs_and_log(self):
        log_records = [pickle.loads(a) for a in self.redis.lrange("log", 0, -1)]

        to_delete = len([a for a in log_records if a.is_older_than(self.config.monitor.observation)])
        self.redis.lpop("log", count=to_delete)

        all_elements = [pickle.loads(a) for a in self.redis.lrange("log", 0, -1)]
        worker_count = self.config.app.worker_count
        print(f"===\ttasks:{self.redis.llen('tasks'):>4}\t w:{worker_count:>3}\tlog:{len(all_elements):>3}\tdel_log:{to_delete}", flush=True)

        return all_elements

    def monitor_cb(self):
        self.app_lock.refresh_lock()

        all_elements = self._get_logs_and_log()

        for item in self.node_wrappers:
            item.process_logs(all_elements)

            latest_block_number = item.get_latest_block_number()

            msg = [f"[{item.type:>5}]", f"[l:{item.logs_count:>4}]"]

            msg.append(f"[{(100.0 * item.max_block_height / latest_block_number):>6.2f}%]")

            msg.append("Δt:")
            msg.append(f"{item.delta_t:>4.1f}" if item.delta_t else "  ??")
            msg.append(f"\tlocal: {item.max_block_height:<8,}".replace(',', " ") )

            if item.max_block_height > -1:
                blocks_left = latest_block_number - item.max_block_height
                msg.append(f"\tleft: {blocks_left:<8,}".replace(',', ' '))

            if item.logs_count > 1:
                msg.append("===")
                msg.append(f"{item.blocs_per_hour / 60.0:>4.0f} b/min  ")

                progress_per_hour = (item.blocs_per_hour / latest_block_number) * 100.0
                msg.append(f"{progress_per_hour:>4.2f} %/h  ")

                eta = ((latest_block_number - item.max_block_height) / item.blocs_per_hour) / 24.0
                if eta > 1.0:
                    msg.append(f"eta: {eta:3.1f} days")
                else:
                    eta = eta / 24.0
                    msg.append(f"eta: {eta:2.1f} hours")


            print(f"\t{' '.join(msg)}", flush=True)

        print()
