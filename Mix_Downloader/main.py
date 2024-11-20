
from ROTools.Helpers.Info import print_info
from ROTools.Helpers.WorkersCollection import WorkersCollection

from source_code.Steps.PriceStep import PriceStep
from source_code.helpers.config_builder import load_config
from source_code.workers.Worker import Worker

if __name__ == '__main__':
    import pickle
    import multiprocessing
    multiprocessing.set_start_method('spawn')

    from source_code.workers.MonitorWorker import MonitorWorker

    print_info()
    config = load_config()
    monitor = MonitorWorker(config)
    redis = config.get_redis()

    config.get_minio().create_bucket(config.metal_price_api.bucket_name)

    print("Init")
    monitor.init()

    print("START")

    redis.rpush("tasks", pickle.dumps(PriceStep))
    workers = WorkersCollection()
    workers.add(Worker, config.app.worker_count)
    workers.start()
    workers.monitor(cb=monitor.monitor_cb, sleep_time=config.app.monitor_refresh_time)
    workers.join()


    monitor.app_lock.release_lock()

