from datetime import datetime


class ProgresLog:
    def __init__(self, name, height):
        self.name = name
        self.height = height
        self.time = datetime.now()

    def is_older_than(self, time_delta_seconds):
        _delta = datetime.now() - self.time
        return _delta.total_seconds() > time_delta_seconds

    def get_time_delta(self, other):
        delta = (self.time - other.time).total_seconds()
        return abs(delta)
