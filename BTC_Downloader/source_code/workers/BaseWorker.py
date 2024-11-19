import sys
import time
import traceback

from source_code.config.Config import ConfigLoader



def get_block_object_name(height):
    number_text = f"{height:08d}"
    s1 = number_text[:4]
    return f"blocks_{s1}/block_{number_text}.json"


class BaseWorker:
    def __init__(self, stop_event, name, index):
        self.name = f"{name}-{index}"
        self.stop_event = stop_event
        self.config = ConfigLoader().get_data()
        self.local_config = getattr(self.config, name)

        self._was_wait = False
        self._sleep_counter = 1

    @classmethod
    def start(cls, index, stop_event):
        try:
            worker = cls(index, stop_event)

            worker.init()
            while not stop_event.is_set():
                worker._base_step()

        except KeyboardInterrupt:
            exit(0)

        except Exception as e:
            print(f"Worker {cls} encountered an error: {repr(e)}", file=sys.stderr)
            traceback.print_exc()
            stop_event.set()
            exit(1)

    def _base_step(self):
        self._was_wait = False
        self.step()
        if self._was_wait:
            self._sleep_counter = min(10, self._sleep_counter + 1)
        else:
            self._sleep_counter = 1


    def _wait_sleep(self):
        self._was_wait = True
        sleep_time = self._sleep_counter * self.config.app.sleep_time
        if self.local_config.sleep_log:
            print(f"{self.name} is waiting... [{sleep_time:>2} s]")

        time.sleep(sleep_time)
