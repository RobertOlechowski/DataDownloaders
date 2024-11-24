from datetime import datetime, timezone

from source_code.Steps.BaseStep import BaseStep


def _get_object_name(name):
    time = datetime.now(timezone.utc)
    return f"symbol/{time.year}_{time.month:02}/{name}.json"


class CmcSymbolStep(BaseStep):
    def __init__(self, config, step_config, mode, status=None, request_wrapper=None):
        super().__init__(config, step_config)

        self.request_wrapper = request_wrapper

        self.name = "CMC"

        if mode == "crypto":
            self.sub_name = f"symbol_{mode}_{status}"
            self.object_name = _get_object_name(f"crypto_{status}")
            self.params = dict(listing_status=status, aux="platform,first_historical_data,last_historical_data,is_active,status", limit=5000)
            self.endpoint = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/map"

        if mode == "fiat":
            self.sub_name = f"symbol_{mode}"
            self.object_name = _get_object_name(f"fiat")
            self.params = dict(include_metals=True, limit=5000)
            self.endpoint = "https://pro-api.coinmarketcap.com/v1/fiat/map"

        self.data_records = []

    @staticmethod
    def get_object_name_crypto(status):
        object_name = _get_object_name(f"crypto_{status}")
        return object_name


    def init_impl(self):
        if self.minio.object_exists(self.bucket_name, self.object_name):
            self.is_done = True
            self.send_log(phase=self.sub_name, is_skipped=True)

    def process(self):
        self.params["start"] = len(self.data_records) + 1
        json_data = self.request_wrapper.get_data(endpoint=self.endpoint, params=self.params)
        self.data_records.extend(json_data)

        self.is_done = len(json_data) == 0

        if  self.is_done:
            self.minio.put_json(self.bucket_name, self.object_name, self.data_records)

        self.send_log(phase=self.sub_name, progress=len(self.data_records))
