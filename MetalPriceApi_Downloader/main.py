from ROTools.Config.ConfigLoader import build_config
from ROTools.Helpers.Info import print_info
from ROTools.Helpers.WorkersCollection import WorkersCollection

from source_code.Steps.PriceStep import PriceStep
from source_code.workers.Worker import Worker

if __name__ == '__main__':
    import pickle
    import multiprocessing
    multiprocessing.set_start_method('spawn')

    from source_code.workers.MonitorWorker import MonitorWorker

    print_info()
    config = build_config(file_name='config/config.yaml', skip_dump=False, prefix="METAL2DB_")
    monitor = MonitorWorker(config)
    redis = config.get_redis()

    config.get_minio().create_bucket(config.app.bucket_name)

    print("Init")
    monitor.init()

    print("START")

    redis.rpush("tasks", pickle.dumps(PriceStep()))
    workers = WorkersCollection()
    workers.add(Worker, 1)
    workers.start()
    workers.monitor(cb=monitor.monitor_cb, sleep_time=config.app.monitor_refresh_time)
    workers.join()


    monitor.app_lock.release_lock()

