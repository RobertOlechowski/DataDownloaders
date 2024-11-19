import sys
import time
import traceback

from source_code.config.Config import ConfigLoader

def get_block_object_name(height):
    number_text = f"{height:010d}"
    s1 = number_text[:6]
    return f"blocks_{s1}/block_{number_text}.json"


class BaseWorker:
    def __init__(self, stop_event, name, index):
        self.name = f"{name}-{index}"
        self.stop_event = stop_event
        self.config = ConfigLoader().get_data()
        self.local_config = getattr(self.config, name)

    @classmethod
    def start(cls, index, stop_event):
        try:
            worker = cls(index, stop_event)

            worker.init()
            while not stop_event.is_set():
                worker.step()

        except KeyboardInterrupt:
            # print(f"\nWorker {cls.__name__} received KeyboardInterrupt")
            exit(0)

        except Exception as e:
            print(f"Worker {cls} encountered an error: {repr(e)}", file=sys.stderr)
            traceback.print_exc()
            stop_event.set()
            exit(1)

    def _wait_sleep(self):
        if self.local_config.sleep_log:
            print(f"{self.name} is waiting...")

        time.sleep(self.local_config.sleep_time)
