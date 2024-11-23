import pickle

from source_code.Msg.ProgresLog import ProgresLog


class BaseStep:
    def __init__(self, config, step_config):
        self.name = None
        self.config, self.step_config = config, step_config

        self.minio = config.get_minio()
        self.redis = config.get_redis()

        self.bucket_name = getattr(step_config, "bucket_name", None)
        self.init_done, self.is_done = False, False

        self.steps = None
        self.sub_name = None

    def init(self):
        self.init_done = True

        if hasattr(self, 'init_impl'):
            self.init_impl()

    def _run_steps_in_this_thread(self):
        self.send_log(phase=self.sub_name, progress=len(self.steps))

        if len(self.steps) == 0:
            self.is_done = True
            self.send_log(phase=self.sub_name)
            return

        step_class, params = self.steps.pop(0)
        step_task = step_class(self.config, self.step_config, request_wrapper=self.request_wrapper,  **params)
        step_task.init()

        if step_task.is_done:
            return

        while True:
            step_task.process()
            if step_task.is_done:
                break

    def _build_status(self, is_skipped, is_started):
        if is_skipped:
            return "SKIPPED"

        if is_started:
            return "START"

        if self.is_done:
            return "DONE"

        return "PROGRESS"

    def send_log(self, step_name=None, phase=None, is_skipped=False, progress_text=None, is_started=False, progress=0):
        step_name = step_name or self.name
        phase = phase or self.sub_name

        status = self._build_status(is_skipped, is_started)

        log = ProgresLog(name=step_name,
                         sub_name=phase,
                         progress=progress,
                         progress_text=progress_text,
                         status=status)
        self.redis.rpush("log", pickle.dumps(log))
