import pickle

from ROTools.Helpers.RateLimiter import RateLimiter

from source_code.helpers.RequestWrapper import RequestWrapper
from source_code.workers.BaseWorker import BaseWorker


class Worker(BaseWorker):
    def __init__(self, index, stop_event):
        super().__init__(stop_event, "task_producer", index)

        self.redis = self.config.get_redis()
        self.request_wrapper = RequestWrapper(self.config)
        self.rate_limiter = RateLimiter(self.config.app.request_period_limit, show_wait=False)
        self.minio = self.config.get_minio()
        self.context = None

    def init(self):
        pass

    def _get_task(self):
        if self.context is None:
            step_task = self.redis.lpop("tasks")
            if step_task is None:
                return None
            step_task = pickle.loads(step_task)
            step_task.redis = self.redis
            step_task.minio = self.minio
            step_task.config = self.config
            step_task.request_wrapper = self.request_wrapper
            step_task.rate_limiter = self.rate_limiter
            step_task.init()
            self.context = step_task
        return self.context

    def step(self):
        step_task = self._get_task()
        if step_task is None:
            self._wait_for_data()
            return

        if not step_task.is_done:
            step_task.process()

        if step_task.is_done:
            self.context = None
