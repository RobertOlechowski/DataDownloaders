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
        if self.context is None:
            step_task_class = self.redis.lpop("tasks")
            if step_task_class is None:
                return None
            step_task_class = pickle.loads(step_task_class)
            step_task = step_task_class(self.config)
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
