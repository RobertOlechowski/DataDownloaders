import pickle

from source_code.Msg.ProgresLog import ProgresLog


class BaseStep:
    def __init__(self, config):
        self.config = config
        self.init_done, self.is_done = False, False

    def init(self):
        self.init_done = True

        if hasattr(self, 'init_impl'):
            self.init_impl()

    def send_log(self, name=None, sub_name="", is_done=False, progress_text=None, progress=0):
        log = ProgresLog(name=name,
                         sub_name=sub_name,
                         progress=progress,
                         progress_text=progress_text,
                         is_done=is_done,
                         skip=False)
        self.redis.rpush("log", pickle.dumps(log))
