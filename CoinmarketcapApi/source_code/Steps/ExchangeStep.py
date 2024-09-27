from datetime import datetime, timezone

from source_code.Steps.BaseStep import BaseStep


def _get_object_name(name):
    time = datetime.now(timezone.utc)
    return f"exchange/{time.year}_{time.month:02}/{name}.json"


class ExchangeStep(BaseStep):
    def __init__(self, status=None):
        super().__init__()

        self.main_name = "Exchange"
        self.endpoint = " https://pro-api.coinmarketcap.com/v1/exchange/map"
        self.params = dict(limit=500, listing_status=status, aux="first_historical_data,last_historical_data,is_active,status")

        self.sub_name = f"exchange_{status}"
        self.object_name = _get_object_name(f"{status}")

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
