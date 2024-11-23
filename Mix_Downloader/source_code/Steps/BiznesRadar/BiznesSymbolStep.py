from datetime import datetime, timezone

from ROTools.Helpers.DictObj import DictObj
from ROTools.Helpers.RateLimiter import RateLimiter

from source_code.Steps.Cmc.CmcRequestWrapper import CmcRequestWrapper
from source_code.Steps.BaseStep import BaseStep


class BiznesSymbolStep(BaseStep):
    def __init__(self, config, step_config, request_wrapper=None):
        super().__init__(config, step_config)

        self.request_wrapper = request_wrapper

        self.name = "Biznes"
        self.sub_name = f"Symbols"
        self.object_name = self.get_object_name()

    @staticmethod
    def get_object_name():
        time = datetime.now(timezone.utc)
        return f"symbols/{time.year}/{time.year}_{time.month:02}_{time.day:02}.json"

    def init_impl(self):
        if self.minio.object_exists(self.bucket_name, self.object_name):
            self.is_done = True
            self.send_log(phase=self.sub_name, is_skipped=True)
            return
        self.send_log(phase=self.sub_name, is_started=True)

    def process(self):
        symbols = []
        for symbol_type in ["GPW", "NewConnect", "CFD", "Index"]:
            _symbols = self.request_wrapper.get_symbols(symbol_type=symbol_type)
            self.send_log(phase=self.sub_name, progress_text=symbol_type)
            symbols.extend(_symbols)

        self.minio.put_json(self.step_config.bucket_name, self.object_name, symbols)
        self.is_done = True
        self.send_log(phase=self.sub_name, progress=len(_symbols))
