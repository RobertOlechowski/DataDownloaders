import os
import signal
import multiprocessing

from source_code.config.Config import ConfigLoader
from source_code.workers.MonitorWorker import MonitorWorker
from source_code.helpers.WorkersCollection import WorkersCollection
from source_code.workers.IdProducer import IdProducer
from source_code.workers.BlockDownloader import BlockDownloader

if __name__ == '__main__':
    multiprocessing.set_start_method('spawn')

    config_loader = ConfigLoader()
    config = config_loader.get_data()
    redis = config.get_redis()
    monitor = MonitorWorker(config)

    print("Init")
    monitor.init()

    print("START")

    _queue_names = ["block_ids", "block_data", "log"]
    for item in _queue_names:
        redis.delete(item)

    minio = config.get_minio()
    if minio is not None:
        minio.create_bucket("btc-cache")

    workers = WorkersCollection()
    workers.add(IdProducer, 1, start=True)
    workers.add(BlockDownloader, config.app.downloader_count)

    def stop_app():
        print(f"Parent process (PID: {os.getpid()}) received SIGTERM. Waiting for workers to finish...")
        workers.stop()

    signal.signal(signal.SIGTERM, lambda a, b : stop_app())
    signal.signal(signal.SIGINT, lambda a, b: stop_app())

    workers.start()
    workers.monitor(cb=monitor.monitor_cb, config=config)
    workers.join()

    monitor.app_lock.release_lock()

