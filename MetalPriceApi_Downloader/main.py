from ROTools.Config.ConfigLoader import build_config
from ROTools.Helpers.Info import print_info
from ROTools.Helpers.WorkersCollection import WorkersCollection

from source_code.Steps.SymbolStep import SymbolStep
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

    if config.tasks.symbols:
        redis.rpush("tasks", pickle.dumps(SymbolStep()))
        workers = WorkersCollection()
        workers.add(Worker, 1)
        workers.start()
        workers.monitor(cb=monitor.monitor_cb, sleep_time=config.app.monitor_refresh_time)
        workers.join()


    if config.tasks.price:
        _steps.append(ExchangeStep(status="active"))


    #workers = WorkersCollection()
    #workers.add(Worker, config.app.downloader_count)
    #workers.start()
    #workers.monitor(cb=monitor.monitor_cb, sleep_time=config.app.monitor_refresh_time)
    #workers.join()

    monitor.app_lock.release_lock()

