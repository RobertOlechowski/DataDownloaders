import sys
import time
import traceback

from source_code.helpers.config_builder import load_config


class BaseWorker:
    def __init__(self, stop_event, name, index):
        self.name = f"{name}-{index}"
        self.stop_event = stop_event
        self.config = load_config()

    @classmethod
    def start(cls, index, stop_event):
        try:
            worker = cls(index, stop_event)

            worker.init()
            while not stop_event.is_set():
                worker.step()

        except KeyboardInterrupt:
            exit(0)

        except Exception as e:
            print(f"Worker {cls} encountered an error: {repr(e)}", file=sys.stderr)
            traceback.print_exc()
            stop_event.set()
            exit(1)

    def _wait_for_data(self):
        time.sleep(self.config.app.no_data_sleep_time)
