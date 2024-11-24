import pickle

from source_code.workers.BaseWorker import BaseWorker


class Worker(BaseWorker):
    def __init__(self, index, stop_event):
        super().__init__(stop_event, "worker", index)
        self.context = None

    def init(self):
        pass

    def _set_context(self):
        if self.context is not None:
            return

        task_raw = self.redis.lpop("tasks")
        if task_raw is None:
            return None
        step_class, step_config, params = pickle.loads(task_raw)
        params = params or {}
        step_task = step_class(self.config, step_config, **params)
        step_task.init()
        self.context = step_task
        self.redis.sadd("workers", self.name)


    def step(self):
        self._set_context()

        if self.context is None:
            self._wait_for_data()
            return

        if not self.context.is_done:
            self.context.process()
        else:
            self.context = None
            self.redis.srem("workers", self.name)

