from datetime import datetime, timezone

from source_code.Steps.BaseStep import BaseStep
from source_code.Steps.Farside.FarsideRequestWrapper import FarsideRequestWrapper


class FarsideStep(BaseStep):
    def __init__(self, config, step_config):
        super().__init__(config, step_config)

        self.name = "Farside"
        self.coin = step_config.path

        self.request_wrapper = FarsideRequestWrapper(self.step_config)
        self.bucket_name = step_config.bucket_name

    def process(self):
        time = datetime.now(timezone.utc)
        time_text = time.date().isoformat()
        object_name = f"{time.year}/{self.step_config.path}_{time_text}.json"

        if self.minio.object_exists(self.bucket_name, object_name):
            self.is_done = True
            self.send_log(phase=self.coin, is_skipped=True)
            return

        data = self.request_wrapper.get_data()

        self.minio.put_json(self.bucket_name, object_name, data)
        self.is_done = True
        self.send_log(phase=self.coin)

