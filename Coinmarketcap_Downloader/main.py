import os
import pickle
import signal

from ROTools.Helpers.WorkersCollection import WorkersCollection

from source_code.Steps.AssetsStep import AssetsStep
from source_code.Steps.StepBuilderStep import StepBuilderStep
from source_code.Steps.ExchangeStep import ExchangeStep
from source_code.Steps.SymbolStep import SymbolStep
from source_code.helpers.RequestWrapper import RequestWrapper

if __name__ == '__main__':
    import multiprocessing
    multiprocessing.set_start_method('spawn')

    from source_code.config.Config import ConfigLoader
    from source_code.workers.MonitorWorker import MonitorWorker
    from source_code.workers.Worker import Worker

    config_loader = ConfigLoader()
    config = config_loader.get_data()
    monitor = MonitorWorker(config)
    request_wrapper = RequestWrapper(config)

    print("Init")
    monitor.init()

    print("START")

    _steps = []

    if config.tasks.symbols:
        _steps.append(SymbolStep(mode="crypto", status="active"))
        _steps.append(SymbolStep(mode="crypto", status="inactive"))
        _steps.append(SymbolStep(mode="crypto", status="untracked"))
        _steps.append(SymbolStep(mode="fiat"))

    if config.tasks.exchanges:
        _steps.append(ExchangeStep(status="active"))
        _steps.append(ExchangeStep(status="inactive"))
        _steps.append(ExchangeStep(status="untracked"))

    if config.tasks.assets:
        _steps.append(AssetsStep())

    if config.tasks.ohlcv_daily:
        _steps.append(StepBuilderStep(mode="OHLCV", interval="daily"))

    if config.tasks.ohlcv_hourly:
        _steps.append(StepBuilderStep(mode="OHLCV", interval="hourly"))

    redis = config.get_redis()


    redis.rpush("tasks", *[pickle.dumps(a) for a in _steps])

    workers = WorkersCollection()
    workers.add(Worker, 1)

    def stop_app():
        print(f"Parent process (PID: {os.getpid()}) received SIGTERM. Waiting for workers to finish...")
        workers.stop()

    signal.signal(signal.SIGTERM, lambda a, b : stop_app())
    signal.signal(signal.SIGINT, lambda a, b: stop_app())

    workers.start()
    workers.monitor(cb=monitor.monitor_cb, sleep_time=config.app.monitor_refresh_time)
    workers.join()

    monitor.app_lock.release_lock()

