from datetime import datetime, timezone

from source_code.Steps.BaseStep import BaseStep
from source_code.Steps.ExchangeStep import ExchangeStep
from source_code.helpers.DictObj import DictObj


def _get_object_name(name):
    time = datetime.now(timezone.utc)
    return f"assets/{time.year}_{time.month:02}_{name}.json"


class AssetsStep(BaseStep):
    def __init__(self):
        super().__init__()

        self.main_name = "Assets"
        self.sub_name = ""

        self.endpoint = "https://pro-api.coinmarketcap.com/v1/exchange/assets"
        self.exchange_id = None
        self.params = None

        self.object_name = _get_object_name(f"assets")

        self.exchange_list = []
        self.data_records = []

    def init_impl(self):
        if self.minio.object_exists(self.bucket_name, self.object_name):
            self.is_done = True
            self.send_log(progress=-1)
            return

        _temp_step = ExchangeStep(status="active")
        exchanges = self.minio.get_json(_temp_step.bucket_name, _temp_step.object_name)
        exchanges = [DictObj(a) for a in exchanges]

        self.exchange_list = [a.id for a in exchanges]

    def _get_data(self):
        self.exchange_id = self.exchange_list.pop(0)
        self.params = dict(id=self.exchange_id)
        return self.request_wrapper.get_data(request_data=self)

    def process(self):
        self.rate_limiter.call_wait()
        json_data = self._get_data()
        self.data_records.append(dict(exchange_id=self.exchange_id, data=json_data))

        if len(self.exchange_list) == 0:
            self.is_done = True

        if not self.is_done:
            self.send_log(progress=len(self.exchange_list))
            return

        self.minio.put_json(self.bucket_name, self.object_name, self.data_records)
        self.send_log(progress=len(self.exchange_list))
