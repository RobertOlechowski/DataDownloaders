from ROTools.Helpers.RateLimiter import RateLimiter
from source_code.Steps.BaseStep import BaseStep
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

        self.steps = []

        if self.step_config.tasks.symbols:
            self._add_task((BiznesSymbolStep, dict()))

        if self.step_config.tasks.recommendations:
            self._add_task((BiznesRecommendationsStep, dict()))

    def _add_task(self, task):
        self.steps.append(task)

    def process(self):
        self._run_steps_in_this_thread()


