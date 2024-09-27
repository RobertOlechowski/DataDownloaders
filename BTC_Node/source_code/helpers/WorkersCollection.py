import sys
import time
from multiprocessing import Process, Event


class WorkersCollection:
    def __init__(self):
        self.stop_event = Event()
        self.process = []
        self._is_running = False

    def add(self, worker_class, count, start=False):

        for index in range(count):
            process = Process(target=worker_class.start, args=(index, self.stop_event,))
            if start:
                process.start()
            self.process.append(process)

    def stop(self, ):
        self.stop_event.set()

    def start(self, ):
        for process in self.process:
            if process.exitcode is None and not process.is_alive():
                process.start()

    def join(self, ):
        self.stop()
        for item in self.process:
            item.join()

    def is_running(self):
        return all([a.is_alive() for a in self.process])

    def check_error(self):
        error = len([a for a in self.process if a.exitcode is not None and a.exitcode != 0]) > 0
        if error:
            self.stop()

    def monitor(self, cb, config):
        sleep_time = config.app.monitor_refresh_time
        try:
            while self.is_running():
                time.sleep(sleep_time)
                cb()
        except KeyboardInterrupt:
            print("\nMain process received KeyboardInterrupt, terminating workers...")

            self.stop()
            self.join()
            print("All workers terminated. Main process exiting.")
        except Exception as e:
            print("Monitor Error", file=sys.stderr)
            self.stop()
            raise e
