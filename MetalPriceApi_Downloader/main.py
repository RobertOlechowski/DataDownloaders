

if __name__ == '__main__':
    import pickle
    import multiprocessing
    multiprocessing.set_start_method('spawn')

    from source_code.config.Config import ConfigLoader
    from source_code.workers.MonitorWorker import MonitorWorker
    from source_code.helpers.WorkersCollection import WorkersCollection
    from source_code.helpers.RequestWrapper import RequestWrapper

    config_loader = ConfigLoader()
    config = config_loader.get_data()
    monitor = MonitorWorker(config)
    request_wrapper = RequestWrapper(config)

    print("Init")
    monitor.init()

    print("START")

    _steps = []

    #if config.tasks.symbols:
    #    _steps.append(SymbolStep(mode="crypto", status="active"))

    #if config.tasks.price:
    #    _steps.append(ExchangeStep(status="active"))


    redis = config.get_redis()

    #redis.rpush("tasks", *[pickle.dumps(a) for a in _steps])

    workers = WorkersCollection()

    #workers.add(Worker, 1)

    workers.start()
#    workers.monitor(cb=monitor.monitor_cb, sleep_time=config.app.monitor_refresh_time)
    workers.join()

    monitor.app_lock.release_lock()

