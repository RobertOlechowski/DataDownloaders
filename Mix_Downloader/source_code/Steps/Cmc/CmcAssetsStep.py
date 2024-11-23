from datetime import datetime, timezone

from ROTools.Helpers.DictObj import DictObj

from source_code.Steps.BaseStep import BaseStep
from source_code.Steps.Cmc.CmcExchangeStep import CmcExchangeStep


def _get_object_name(name):
    time = datetime.now(timezone.utc)
    return f"assets/{time.year}_{time.month:02}_{name}.json"


class CmcAssetsStep(BaseStep):
    def __init__(self, config, step_config, request_wrapper):
        super().__init__(config, step_config)

        self.request_wrapper = request_wrapper

        self.name = "CMC"
        self.sub_name = "Assets"

        self.endpoint = "https://pro-api.coinmarketcap.com/v1/exchange/assets"
        self.exchange_id = None
        self.params = None

        self.object_name = _get_object_name(f"assets")

        self.exchange_list = []
        self.data_records = []

    def init_impl(self):
        if self.minio.object_exists(self.bucket_name, self.object_name):
            self.is_done = True
            self.send_log(phase=self.sub_name, is_skipped=True)
            return

        exchange_object_name = CmcExchangeStep(self.config, self.step_config, status="active").object_name
        exchanges = self.minio.get_json(self.bucket_name, exchange_object_name)
        exchanges = [DictObj(a) for a in exchanges]

        self.exchange_list = [a.id for a in exchanges]


    def process(self):
        self.exchange_id = self.exchange_list.pop(0)
        self.params = dict(id=self.exchange_id)
        json_data = self.request_wrapper.get_data(endpoint=self.endpoint, params=self.params)

        self.data_records.append(dict(exchange_id=self.exchange_id, data=json_data))

        self.is_done = len(self.exchange_list) == 0

        if self.is_done:
            self.minio.put_json(self.bucket_name, self.object_name, self.data_records)

        self.send_log(phase=self.sub_name, progress=len(self.exchange_list))

