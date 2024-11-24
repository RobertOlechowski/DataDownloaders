import os
import signal
import multiprocessing

from source_code.config.Config import ConfigLoader
from source_code.workers.MonitorWorker import MonitorWorker
from source_code.workers.IdProducer import IdProducer
from source_code.workers.Worker import Worker
from ROTools.Helpers.WorkersCollection import WorkersCollection

if __name__ == '__main__':
    multiprocessing.set_start_method('spawn')

    config_loader = ConfigLoader()
    config = config_loader.get_data()
    redis = config.get_redis()
    monitor = MonitorWorker(config)

    print("Init")
    monitor.init()

    print("START")

    _queue_names = ["tasks", "log"]
    for item in _queue_names:
        redis.delete(item)

    minio = config.get_minio()
    for item_name in config.run:
        item = getattr(config.nodes, item_name)
        minio.create_bucket(item.bucket_name)

    workers = WorkersCollection()
    workers.add(IdProducer, 1, start=True)
    workers.add(Worker, config.app.worker_count)

    workers.run(monitor_cb=monitor.monitor_cb, monitor_refresh_time=config.monitor.refresh_time)

    monitor.app_lock.release_lock()
    print("Lock released.")

