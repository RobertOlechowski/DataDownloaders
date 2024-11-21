import pickle

from source_code.Steps.BaseStep import BaseStep
from source_code.Steps.Cmc.CmcAssetsStep import CmcAssetsStep
from source_code.Steps.Cmc.CmcExchangeStep import CmcExchangeStep
from source_code.Steps.Cmc.CmcSymbolStep import CmcSymbolStep


class CmcBuilderStep(BaseStep):
    def __init__(self, config, step_config):
        super().__init__(config, step_config)
        self.name = "CMC"
        self.sub_name = "Step builder"

    def _add_task(self, task):
        self.redis.rpush("tasks", pickle.dumps(task))

    def process(self):
        self.send_log(phase=self.sub_name)
        if self.step_config.tasks.symbols:
            self._add_task((CmcSymbolStep, self.step_config, dict(mode="crypto", status="active")))
            self._add_task((CmcSymbolStep, self.step_config, dict(mode="crypto", status="inactive")))
            self._add_task((CmcSymbolStep, self.step_config, dict(mode="crypto", status="untracked")))
            self._add_task((CmcSymbolStep, self.step_config, dict(mode="fiat")))

        if self.step_config.tasks.exchanges:
            self._add_task((CmcExchangeStep, self.step_config, dict(status="active")))
            self._add_task((CmcExchangeStep, self.step_config, dict(status="inactive")))
            self._add_task((CmcExchangeStep, self.step_config, dict(status="untracked")))

        if self.step_config.tasks.assets:
            self._add_task((CmcAssetsStep, self.step_config, None))


        self.is_done = True
        self.send_log(phase=self.sub_name)






