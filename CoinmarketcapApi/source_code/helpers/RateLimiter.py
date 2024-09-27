import time
from datetime import datetime


class RateLimiter:
    def __init__(self, request_delay, show_wait=True):
        self.request_delay = request_delay
        self.last_call = datetime.fromisoformat("1971-01-01")
        self.show_wait = show_wait

    def call_wait(self):
        _delta = (datetime.now() - self.last_call).total_seconds()
        _wait_time = (self.request_delay / 1000) - _delta
        if _wait_time <= 0:
            self.last_call = datetime.now()
            return
        if self.show_wait:
            print(f"Wait for {_wait_time:>1.2f} seconds")
        time.sleep(_wait_time)
        self.last_call = datetime.now()
