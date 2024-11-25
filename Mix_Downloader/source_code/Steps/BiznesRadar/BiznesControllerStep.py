from datetime import datetime, timezone

from ROTools.Helpers.DictObj import DictObj
from ROTools.Helpers.RateLimiter import RateLimiter

from source_code.Steps.BaseStep import BaseStep
from source_code.Steps.BiznesRadar.BiznesEodStep import BiznesEodStep
from source_code.Steps.BiznesRadar.BiznesRecommendationsStep import BiznesRecommendationsStep
from source_code.Steps.BiznesRadar.BiznesReportStep import BiznesReportStep
from source_code.Steps.BiznesRadar.BiznesRequestWrapper import BiznesRequestWrapper
from source_code.Steps.BiznesRadar.BiznesSymbolStep import BiznesSymbolStep


class BiznesControllerStep(BaseStep):
    def __init__(self, config, step_config, phase = "P1", request_wrapper=None):
        super().__init__(config, step_config)
        self.name = "Biznes"
        self.sub_name = "Controller"

        rate_limiter = RateLimiter(step_config.time_per_request_limit, show_wait=False)
        self.request_wrapper = BiznesRequestWrapper(step_config, rate_limiter)

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
            yield BiznesSymbolStep, dict()

        if self.step_config.tasks.recommendations:
            yield BiznesRecommendationsStep, dict()

        yield BiznesControllerStep, dict(phase="P2")

    def _get_p2(self):
        symbols = self._get_symbols()
        symbols = [a for a in symbols if a.ticker not in self.step_config.skip_eod]

        if self.step_config.tasks.eod:
            for symbol in [a for a in symbols if a.type in ["CFD", "Index"]]: # "GPW", "NewConnect"  "CFD", "Index"
                yield BiznesEodStep, dict(symbol=symbol)

        if self.step_config.tasks.report:
            for symbol in [a for a in symbols if a.type in ["GPW", "NewConnect"]]:
                yield BiznesReportStep, dict(symbol=symbol)

    def _get_symbols(self):
        symbol_object_name = BiznesSymbolStep.get_object_name()
        symbols = self.minio.get_json(self.step_config.bucket_name, symbol_object_name)
        symbols = [DictObj(a) for a in symbols]
        return symbols

    def process(self):
        self._run_steps_in_this_thread()

        if self.is_done:
            self._save_last_refresh_time()


