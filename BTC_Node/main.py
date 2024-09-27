from source_code.DAO.BaseDAO import BaseDAO

if __name__ == '__main__':
    import multiprocessing
    multiprocessing.set_start_method('spawn')

    from source_code.config.Config import ConfigLoader
    from source_code.workers.MonitorWorker import MonitorWorker
    from source_code.helpers.WorkersCollection import WorkersCollection
    from source_code.workers.IdProducer import IdProducer
    from source_code.workers.BlockDownloader import BlockDownloader
    from source_code.workers.BlockInserter import BlockInserter


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

    engine = config.get_db_engine()
    if engine is not None:
        BaseDAO.metadata.create_all(engine)


    workers = WorkersCollection()
    workers.add(IdProducer, 1, start=True)
    workers.add(BlockDownloader, config.app.downloader_count)
    workers.add(BlockInserter, config.app.inserter_count)
    workers.start()
    workers.monitor(cb=monitor.monitor_cb, config=config)
    workers.join()

    monitor.app_lock.release_lock()

