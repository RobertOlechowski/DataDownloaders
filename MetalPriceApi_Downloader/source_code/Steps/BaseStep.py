import pickle

from source_code.Msg.ProgresLog import ProgresLog


class BaseStep:
    def __init__(self):
        self.init_done, self.is_done = False, False
        self.minio, self.request_wrapper, self.rate_limiter, self.redis = None, None, None, None
        self.config = None

    def init(self):
        self.init_done = True

        if hasattr(self, 'init_impl'):
            self.init_impl()

    def send_log(self, progress=None, progress_text=None, skip=None):
        id_number = getattr(self, 'id_number', None)
        interval = getattr(self, 'interval', None)

        log = ProgresLog(name=self.main_name,
                         sub_name=self.sub_name,
                         progress=progress,
                         progress_text=progress_text,
                         is_done=self.is_done,
                         skip=skip,
                         id_number=id_number,
                         interval=interval)
        self.redis.rpush("log", pickle.dumps(log))
