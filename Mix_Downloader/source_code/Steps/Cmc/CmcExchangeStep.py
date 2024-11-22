from datetime import datetime, timezone
from source_code.Steps.BaseStep import BaseStep

def _get_object_name(name):
    time = datetime.now(timezone.utc)
    return f"exchange/{time.year}_{time.month:02}/{name}.json"


class CmcExchangeStep(BaseStep):
    def __init__(self, config, step_config, status=None, request_wrapper=None):
        super().__init__(config, step_config)

        self.request_wrapper = request_wrapper

        self.name = "CMC"
        self.sub_name = f"exchange_{status}"

        self.endpoint = " https://pro-api.coinmarketcap.com/v1/exchange/map"
        self.params = dict(limit=500, listing_status=status, aux="first_historical_data,last_historical_data,is_active,status")

        self.object_name = _get_object_name(f"{status}")
        self.data_records = []

    def init_impl(self):
        if self.minio.object_exists(self.step_config.bucket_name, self.object_name):
            self.is_done = True
            self.send_log(phase=self.sub_name, is_skipped=True)

    def process(self):
        self.params["start"] = len(self.data_records) + 1
        json_data = self.request_wrapper.get_data(endpoint=self.endpoint, params=self.params)

        self.data_records.extend(json_data)

        self.is_done = len(json_data) == 0

        if  self.is_done:
            self.minio.put_json(self.step_config.bucket_name, self.object_name, self.data_records)

        self.send_log(phase=self.sub_name, progress=len(self.data_records))
