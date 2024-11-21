
from ROTools.Helpers.Info import print_info
from ROTools.Helpers.WorkersCollection import WorkersCollection

from source_code.Steps.FarsideStep import FarsideStep
from source_code.Steps.MetalPriceStep import MetalPriceStep
from source_code.helpers.config_builder import load_config
from source_code.workers.Worker import Worker

if __name__ == '__main__':
    import pickle
    import multiprocessing
    multiprocessing.set_start_method('spawn')

    from source_code.workers.MonitorWorker import MonitorWorker

    print_info()
    config = load_config(skip_dump=False)
    monitor = MonitorWorker(config)
    redis = config.get_redis()

    monitor.init()

    stap_lut = {
        "metal_price_api": MetalPriceStep,
        "farside_btc": FarsideStep,
        "farside_eth": FarsideStep,
    }

    print("START")
    configs = ["metal_price_api", "farside_btc", "farside_eth"]
    for config_name in configs:
        step_config = getattr(config, config_name)
        if not step_config.enable:
            continue
        config.get_minio().create_bucket(step_config.bucket_name)
        step_class = stap_lut[config_name]
        redis.rpush("tasks", pickle.dumps((step_class, step_config)))


    workers = WorkersCollection()
    workers.add(Worker, config.app.worker_count)
    workers.start()
    workers.monitor(cb=monitor.monitor_cb, sleep_time=config.app.monitor_refresh_time)
    workers.join()


    monitor.app_lock.release_lock()

