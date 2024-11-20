import os
import signal

from ROTools.Helpers.WorkersCollection import WorkersCollection

if __name__ == '__main__':
    import multiprocessing
    multiprocessing.set_start_method('spawn')

    from source_code.config.Config import ConfigLoader
    from source_code.workers.MonitorWorker import MonitorWorker
    from source_code.workers.IdProducer import IdProducer
    from source_code.workers.BlockDownloader import BlockDownloader

    config_loader = ConfigLoader()
    config = config_loader.get_data()
    redis = config.get_redis()
    minio = config.get_minio()
    monitor = MonitorWorker(config)

    print("Init")
    monitor.init()

    print("START")

    for item in ["block_ids", "block_data", "log"]:
        redis.delete(item)

    if minio is not None:
        minio.create_bucket(config.app.cache_bucket)

    workers = WorkersCollection()
    workers.add(IdProducer, 1)
    workers.add(BlockDownloader, config.app.downloader_count)

    def stop_app():
        print(f"Parent process (PID: {os.getpid()}) received SIGTERM. Waiting for workers to finish...")
        workers.stop()

    signal.signal(signal.SIGTERM, lambda a, b : stop_app())
    signal.signal(signal.SIGINT, lambda a, b: stop_app())

    workers.start()
    workers.monitor(cb=monitor.monitor_cb, sleep_time=config.app.monitor_refresh_time)
    workers.join()

    monitor.app_lock.release_lock()
    print("Lock released.")

