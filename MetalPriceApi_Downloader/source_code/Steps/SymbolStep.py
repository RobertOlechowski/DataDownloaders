from datetime import datetime, timezone

from source_code.Steps.BaseStep import BaseStep


def _get_object_name(name):
    time = datetime.now(timezone.utc)
    return f"symbol/{time.year}_{time.month:02}/{name}.json"


class SymbolStep(BaseStep):
    def __init__(self, mode, status=None):
        super().__init__()

        self.main_name = "Symbol"
        if mode == "crypto":
            self.sub_name = f"{mode}_{status}"
            self.object_name = _get_object_name(f"crypto_{status}")
            self.params = dict(listing_status=status, aux="platform,first_historical_data,last_historical_data,is_active,status", limit=5000)
            self.endpoint = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/map"

        if mode == "fiat":
            self.sub_name = f"{mode}"
            self.object_name = _get_object_name(f"fiat")
            self.params = dict(include_metals=True, limit=5000)
            self.endpoint = "https://pro-api.coinmarketcap.com/v1/fiat/map"

        self.data_records = []

    def init_impl(self):
        if self.minio.object_exists(self.bucket_name, self.object_name):
            self.is_done = True
            self.send_log(progress=-1)

    def _get_data(self):
        self.params["start"] = len(self.data_records) + 1
        return self.request_wrapper.get_data(request_data=self)

    def process(self):
        self.rate_limiter.call_wait()
        json_data = self._get_data()
        self.data_records.extend(json_data)
        if len(json_data) == 0:
            self.is_done = True

        if not self.is_done:
            self.send_log(progress=len(self.data_records))
            return

        self.minio.put_json(self.bucket_name, self.object_name, self.data_records)

        self.send_log(progress=len(self.data_records))
