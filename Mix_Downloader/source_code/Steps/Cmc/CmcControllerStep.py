import pickle

from ROTools.Helpers.DictObj import DictObj
from ROTools.Helpers.RateLimiter import RateLimiter

from source_code.Steps.BaseStep import BaseStep
from source_code.Steps.Cmc.CmcAssetsStep import CmcAssetsStep
from source_code.Steps.Cmc.CmcExchangeStep import CmcExchangeStep
from source_code.Steps.Cmc.CmcOHLCVStep import CmcOHLCVStep
from source_code.Steps.Cmc.CmcRequestWrapper import CmcRequestWrapper
from source_code.Steps.Cmc.CmcSymbolStep import CmcSymbolStep


class CmcControllerStep(BaseStep):
    def __init__(self, config, step_config):
        super().__init__(config, step_config)
        self.name = "CMC"
        self.sub_name = "Controller"

        rate_limiter = RateLimiter(step_config.time_per_request_limit, show_wait=False)
        self.request_wrapper = CmcRequestWrapper(step_config, rate_limiter)

        self.steps = []

        if self.step_config.tasks.symbols:
            self._add_task((CmcSymbolStep, dict(mode="crypto", status="active")))
            self._add_task((CmcSymbolStep, dict(mode="crypto", status="inactive")))
            self._add_task((CmcSymbolStep, dict(mode="crypto", status="untracked")))
            self._add_task((CmcSymbolStep, dict(mode="fiat")))

        if self.step_config.tasks.exchanges:
            self._add_task((CmcExchangeStep, dict(status="active")))
            self._add_task((CmcExchangeStep, dict(status="inactive")))
            self._add_task((CmcExchangeStep, dict(status="untracked")))

        if self.step_config.tasks.assets:
            self._add_task((CmcAssetsStep, dict()))

        if self.step_config.tasks.OHLCV_daily:
            self._add_OHLCV(interval="daily")

        if self.step_config.tasks.OHLCV_hourly:
            self._add_OHLCV(interval="hourly")

    def _add_OHLCV(self, interval):
        if interval not in ["daily", "hourly"]:
            raise Exception("Flow")

        object_name = CmcSymbolStep(self.config, self.step_config, mode="crypto", status="active").object_name
        symbols = self.minio.get_json(self.step_config.bucket_name, object_name)
        symbols = [DictObj(a) for a in symbols]

        symbols = [a for a in symbols if a.rank < self.step_config.symbol_rank_limit]
        symbols.sort(key=lambda a: a.rank)

        for item in symbols:
            self._add_task((CmcOHLCVStep, dict(symbol=item, interval=interval)))

    def _add_task(self, task):
        self.steps.append(task)

    def process(self):
        self.send_log(phase=self.sub_name, progress=len(self.steps))

        if len(self.steps) == 0:
            self.is_done = True
            self.send_log(phase=self.sub_name)
            return

        step_class, params = self.steps.pop(0)
        step_task = step_class(self.config, self.step_config, request_wrapper=self.request_wrapper,  **params)
        step_task.init()

        if step_task.is_done:
            return

        while True:
            step_task.process()
            if step_task.is_done:
                return





