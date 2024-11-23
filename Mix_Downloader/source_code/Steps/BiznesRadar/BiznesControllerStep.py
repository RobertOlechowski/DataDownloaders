from ROTools.Helpers.RateLimiter import RateLimiter
from source_code.Steps.BaseStep import BaseStep
from source_code.Steps.BiznesRadar.BiznesEodStep import BiznesEodStep
from source_code.Steps.BiznesRadar.BiznesRecommendationsStep import BiznesRecommendationsStep
from source_code.Steps.BiznesRadar.BiznesRequestWrapper import BiznesRequestWrapper
from source_code.Steps.BiznesRadar.BiznesSymbolStep import BiznesSymbolStep


class BiznesControllerStep(BaseStep):
    def __init__(self, config, step_config):
        super().__init__(config, step_config)
        self.name = "Biznes"
        self.sub_name = "Controller"

        rate_limiter = RateLimiter(step_config.time_per_request_limit, show_wait=False)
        self.request_wrapper = BiznesRequestWrapper(step_config, rate_limiter)

        self.steps = list(self._get_tasks())

    @staticmethod
    def _get_tasks():
        yield BiznesSymbolStep, dict()
        yield BiznesRecommendationsStep, dict()
        yield BiznesEodStep, dict()

    def process(self):
        self._run_steps_in_this_thread()


