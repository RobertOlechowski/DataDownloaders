from ROTools.Helpers.Info import print_info
from ROTools.Helpers.WorkersCollection import WorkersCollection

from source_code.Steps.Farside.FarsideStep import FarsideStep
from source_code.Steps.Metal.MetalPriceStep import MetalPriceStep
from source_code.Steps.Cmc.CmcBuilderStep import CmcBuilderStep
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
        "coinmarketcap": CmcBuilderStep,
    }

    print("START")

    for config_name in config.run:
        step_config = getattr(config, config_name)
        if not step_config.enable:
            continue
        config.get_minio().create_bucket(step_config.bucket_name)
        redis.rpush("tasks", pickle.dumps((stap_lut[config_name], step_config, None)))


    workers = WorkersCollection()
    workers.add(Worker, config.app.worker_count)
    workers.run(monitor_cb=monitor.monitor_cb, monitor_refresh_time=config.app.monitor_refresh_time)

    monitor.app_lock.release_lock()

