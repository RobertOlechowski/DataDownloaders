import pickle

from source_code.workers.BaseWorker import BaseWorker

class Worker(BaseWorker):
    def __init__(self, index, stop_event):
        super().__init__(stop_event, "task_producer", index)
        self.redis = self.config.get_redis()
        self.context = None

    def init(self):
        pass

    def _get_task(self):
        if self.context is not None:
            return self.context

        task_raw = self.redis.lpop("tasks")
        if task_raw is None:
            return None
        step_class, step_config, params = pickle.loads(task_raw)
        params = params or {}
        step_task = step_class(self.config, step_config, **params)
        step_task.init()
        return step_task

    def step(self):
        self.context = self._get_task()
        if self.context is None:
            self._wait_for_data()
            return

        if not self.context.is_done:
            self.context.process()

        self.context = None if self.context.is_done else self.context
