import pickle

from ROTools.Helpers.DictObj import DictObj

from source_code.Steps.BaseStep import BaseStep
from source_code.Steps.OHLCVStep import OHLCVStep
from source_code.Steps.SymbolStep import SymbolStep


class StepBuilderStep(BaseStep):
    def __init__(self, mode, interval):
        super().__init__()

        if mode != "OHLCV" or interval not in ["daily", "hourly"]:
            raise Exception("Flow")

        self.interval = interval
        self.main_name = "StepBuilder"
        self.sub_name = mode

    def process(self):
        self.is_done = True

        _temp_step = SymbolStep(mode="crypto", status="active")
        symbols = self.minio.get_json(_temp_step.bucket_name, _temp_step.object_name)
        symbols = [DictObj(a) for a in symbols]

        symbols = [a for a in symbols if a.rank < self.config.app.symbol_rank_limit]
        symbols.sort(key=lambda a: a.rank)

        steps = [OHLCVStep(symbol=a, interval=self.interval) for a in symbols]
        self.redis.rpush("tasks", *[pickle.dumps(a) for a in steps])

        self.send_log(progress=len(symbols))
