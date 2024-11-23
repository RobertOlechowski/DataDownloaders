from datetime import datetime, timezone
from itertools import groupby
from ROTools.Helpers.DictObj import DictObj

from source_code.Steps.BiznesRadar.BiznesEodRecord import BiznesEodRecord
from source_code.Steps.BiznesRadar.BiznesSymbolStep import BiznesSymbolStep
from source_code.Steps.BaseStep import BaseStep

def _get_object_name(year, ticker):
    return f"eod/{ticker}/{year}_{ticker}.json"


class BiznesEodStep(BaseStep):
    def __init__(self, config, step_config, request_wrapper=None):
        super().__init__(config, step_config)

        self.request_wrapper = request_wrapper

        self.name = "Biznes"
        self.sub_name = f"EOD"
        self.symbols_count = None
        self.symbols = None
        self.symbol = None
        self.max_pages = None
        self.current_page = None

        self.last_section_last_date = None

        self.data_for_symbol = []

    def _get_symbols(self):
        symbol_object_name = BiznesSymbolStep.get_object_name()
        symbols = self.minio.get_json(self.step_config.bucket_name, symbol_object_name)
        symbols = [DictObj(a) for a in symbols]
        return symbols

    def _save_data(self):
        sorted_records = sorted(self.data_for_symbol, key=lambda x: x.time)

        grouped_by_year = {
            year: list(group)
            for year, group in groupby(sorted_records, key=lambda x: x.time.year)
        }

        for year, records in grouped_by_year.items():
            object_name = _get_object_name(year=year, ticker=self.symbol.ticker)
            data_to_save = [a.to_dict() for a in records]
            self.minio.put_json(self.bucket_name, object_name, data_to_save)

    def _load_last_group_of_data(self):
        self.last_section_last_date = datetime.fromisoformat('1900-01-01').date()
        prefix = f"eod/{self.symbol.ticker}/"
        objects = self.minio.list_objects(self.bucket_name, prefix=prefix, recursive=False)
        objects = [a.object_name.removeprefix(prefix).removesuffix(".json") for a in objects]
        objects = [int(a.split("_")[0]) for a in objects]
        if len(objects) == 0:
            return
        last_year = max(objects)
        object_name = _get_object_name(year=last_year, ticker=self.symbol.ticker)
        last_section_of_data = self.minio.get_json(self.bucket_name, object_name)
        self.last_section_last_date = max([datetime.fromisoformat(a["time"]).date() for a in last_section_of_data])
        self.data_for_symbol = [BiznesEodRecord(DictObj(a), self.symbol) for a in last_section_of_data]


    def process(self):
        if self.symbols is None:
            self.symbols = self._get_symbols()
            self.symbols_count = len(self.symbols)
            self.send_log(progress_text=f"Symbols [{len(self.symbols)}]")
            return

        if len(self.symbols) == 0:
            self.is_done = True
            self.send_log(progress_text=f"[{self.symbols_count}]")
            return

        if self.symbol is None:
            self.symbol = self.symbols.pop(0)
            self.data_for_symbol = []
            self._load_last_group_of_data()
            # todo: check skip

            self.max_pages = self.request_wrapper.get_eod_paging(symbol=self.symbol)
            self.current_page = 1

            self.send_log(progress_text=f"[{self.symbol.ticker:>8}  pages=??? of {self.max_pages:>3}]  [{len(self.symbols):>3} of {self.symbols_count:>3}]")
            return

        page_data = self.request_wrapper.get_eod_data(symbol=self.symbol, index=self.current_page)

        filtered_data = [a for a in page_data if a.time > self.last_section_last_date]

        _stop_early = len(filtered_data) != len(page_data)
        self.data_for_symbol.extend(filtered_data)

        msg = f"[{self.symbol.ticker:>8}  pages={self.current_page:>3} of {self.max_pages:>3}]  [{len(self.symbols):>3} of {self.symbols_count:>3}]"
        self.send_log(progress_text=msg)

        self.current_page = self.current_page + 1
        if self.current_page > self.max_pages or _stop_early:
            self._save_data()
            self.send_log(progress_text=f"[{self.symbol.ticker:>8}  SAVED]  [{len(self.symbols):>3} of {self.symbols_count:>3}]")
            self.symbol = None
            return
