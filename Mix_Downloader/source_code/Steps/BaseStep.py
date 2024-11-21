import pickle

from source_code.Msg.ProgresLog import ProgresLog


class BaseStep:
    def __init__(self, config):
        self.name = None
        self.config = config
        self.init_done, self.is_done = False, False

    def init(self):
        self.init_done = True

        if hasattr(self, 'init_impl'):
            self.init_impl()

    def send_log(self, step_name=None, phase="", is_done=False, progress_text=None, progress=0):
        step_name = step_name or self.name

        log = ProgresLog(name=step_name,
                         sub_name=phase,
                         progress=progress,
                         progress_text=progress_text,
                         is_done=is_done,
                         skip=False)
        self.redis.rpush("log", pickle.dumps(log))
