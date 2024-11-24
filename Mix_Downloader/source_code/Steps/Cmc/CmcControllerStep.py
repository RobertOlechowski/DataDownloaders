from datetime import datetime, timezone

from ROTools.Helpers.DictObj import DictObj
from ROTools.Helpers.RateLimiter import RateLimiter

from source_code.Steps.BaseStep import BaseStep
from source_code.Steps.Cmc.CmcAssetsStep import CmcAssetsStep
from source_code.Steps.Cmc.CmcExchangeStep import CmcExchangeStep
from source_code.Steps.Cmc.CmcOHLCVStep import CmcOHLCVStep
from source_code.Steps.Cmc.CmcRequestWrapper import CmcRequestWrapper
from source_code.Steps.Cmc.CmcSymbolStep import CmcSymbolStep


class CmcControllerStep(BaseStep):
    def __init__(self, config, step_config, phase = "P1", request_wrapper=None):
        super().__init__(config, step_config)
        self.name = "CMC"
        self.sub_name = f"Controller {phase}"

        rate_limiter = RateLimiter(step_config.time_per_request_limit, show_wait=False)
        self.request_wrapper = CmcRequestWrapper(step_config, rate_limiter)

        if phase == "P1":
            self.steps = list(self._get_p1())

        if phase == "P2":
            self.steps = list(self._get_p2())

    def init_impl(self):
        days_delta = (datetime.now(timezone.utc).date() - self._get_last_refresh_time()).days
        if days_delta <= self.step_config.global_refresh_threshold_days:
            self.is_done = True
            self.send_log(is_skipped=True)
            return

        self.send_log(is_started=True)



    def _get_p1(self):
        if self.step_config.tasks.symbols:
            yield CmcSymbolStep, dict(mode="crypto", status="active")
            yield CmcSymbolStep, dict(mode="crypto", status="inactive")
            yield CmcSymbolStep, dict(mode="crypto", status="untracked")
            yield CmcSymbolStep, dict(mode="fiat")

        if self.step_config.tasks.exchanges:
            yield CmcExchangeStep, dict(status="active")
            yield CmcExchangeStep, dict(status="inactive")
            yield CmcExchangeStep, dict(status="untracked")

        if self.step_config.tasks.assets:
            yield CmcAssetsStep, dict()

        yield CmcControllerStep, dict(phase="P2")

    def _get_p2(self):
        object_name = CmcSymbolStep.get_object_name_crypto(status="active")
        symbols = self.minio.get_json(self.step_config.bucket_name, object_name)
        symbols = [DictObj(a) for a in symbols]

        symbols = [a for a in symbols if a.rank < self.step_config.symbol_rank_limit]
        symbols.sort(key=lambda a: a.rank)

        for item in symbols:
            if self.step_config.tasks.OHLCV_daily:
                yield CmcOHLCVStep, dict(symbol=item, interval="daily")
            if self.step_config.tasks.OHLCV_hourly:
                yield CmcOHLCVStep, dict(symbol=item, interval="hourly")

    def process(self):
        self._run_steps_in_this_thread()

        if self.is_done and all(self.step_config.tasks):
            self._save_last_refresh_time()








