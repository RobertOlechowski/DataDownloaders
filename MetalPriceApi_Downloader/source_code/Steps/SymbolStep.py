from datetime import datetime, timezone

from source_code.Steps.BaseStep import BaseStep


class SymbolStep(BaseStep):
    def __init__(self):
        super().__init__()

        self.main_name = "Symbols"
        self.sub_name = ""
        time = datetime.now(timezone.utc)
        self.object_name = f"symbols/{time.year}_{time.month:02}.json"
        self.data_records = []

    def init_impl(self):
        if self.minio.object_exists(self.config.app.bucket_name, self.object_name):
            self.is_done = True
            self.send_log(progress=-1)

    def process(self):
        self.data_records = self.request_wrapper.get_symbols()
        self.is_done = True

        self.minio.put_json(self.config.app.bucket_name, self.object_name, self.data_records)
        self.send_log(progress=len(self.data_records))
