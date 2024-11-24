import pickle
from datetime import datetime, timezone

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
        self.step_task = None

    def init(self):
        self.init_done = True

        if hasattr(self, 'init_impl'):
            self.init_impl()

    def _get_last_refresh_time(self):
        data = self.minio.get_json(self.bucket_name, "meta.json")
        if data is None:
            return datetime.fromisoformat('1900-01-01').date()
        return datetime.fromisoformat(data['last_refresh_time']).date()

    def _save_last_refresh_time(self):
        last_refresh_time = datetime.now(timezone.utc).date().isoformat()
        self.minio.put_json(self.bucket_name, "meta.json", dict(last_refresh_time=last_refresh_time))

    def _run_steps_in_this_thread(self):
        self.send_log(phase=self.sub_name, progress=len(self.steps))

        if self.step_task is None:
            if len(self.steps) == 0:
                self.is_done = True
                self.send_log(phase=self.sub_name)
                return

            step_class, params = self.steps.pop(0)
            self.step_task = step_class(self.config, self.step_config, request_wrapper=self.request_wrapper,  **params)
            self.step_task.init()

        if self.step_task.is_done:
            self.step_task = None
            return

        self.step_task.process()
        if self.step_task.is_done:
            self.step_task = None


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
        phase = phase or self.sub_name or ""

        status = self._build_status(is_skipped, is_started)

        log = ProgresLog(name=step_name,
                         sub_name=phase,
                         progress=progress,
                         progress_text=progress_text,
                         status=status)
        self.redis.rpush("log", pickle.dumps(log))
