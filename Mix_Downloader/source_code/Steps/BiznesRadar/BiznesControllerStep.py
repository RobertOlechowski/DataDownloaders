from ROTools.Helpers.DictObj import DictObj
from ROTools.Helpers.RateLimiter import RateLimiter
from source_code.Steps.BaseStep import BaseStep
from source_code.Steps.BiznesRadar.BiznesEodStep import BiznesEodStep
from source_code.Steps.BiznesRadar.BiznesRecommendationsStep import BiznesRecommendationsStep
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
            for symbol in symbols:
                yield BiznesEodStep, dict(symbol=symbol)

    def _get_symbols(self):
        symbol_object_name = BiznesSymbolStep.get_object_name()
        symbols = self.minio.get_json(self.step_config.bucket_name, symbol_object_name)
        symbols = [DictObj(a) for a in symbols]
        return symbols

    def process(self):
        self._run_steps_in_this_thread()


