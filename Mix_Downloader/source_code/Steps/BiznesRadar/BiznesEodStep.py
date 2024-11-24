from datetime import datetime, timezone
from itertools import groupby

from ROTools.Helpers.DictObj import DictObj

from source_code.Steps.BaseStep import BaseStep
from source_code.Steps.BiznesRadar.BiznesEodRecord import BiznesEodRecord


def _get_object_name(year, symbol):
    return f"{_get_symbol_dir(symbol)}/{year}_{symbol.ticker}.json"

def _get_meta_object_name(symbol):
    return f"{_get_symbol_dir(symbol)}/meta.json"

def _get_symbol_dir(symbol):
    return f"eod/{symbol.ticker}"

def _get_dir_prefix(symbol):
    return f"{_get_symbol_dir(symbol)}/"

class BiznesEodStep(BaseStep):
    def __init__(self, config, step_config, symbol=None, request_wrapper=None):
        super().__init__(config, step_config)

        self.request_wrapper = request_wrapper

        self.name = "Biznes"
        self.sub_name = f"EOD {symbol.ticker}"
        self.symbol = symbol

        self.max_pages = None
        self.current_page = 1


        self.last_section_last_date = datetime.fromisoformat('1900-01-01').date()
        self.data_for_symbol = []

    def init_impl(self):
        self._load_last_group_of_data()

        last_date = max(self.last_section_last_date, self.get_last_refresh_time())
        days_delta = (datetime.now(timezone.utc).date() - last_date).days
        if days_delta <= self.step_config.refresh_threshold_days:
            self.is_done = True
            self.send_log(is_skipped=True)
            return

        self.send_log(is_started=True)


    def process(self):
        self.max_pages, page_data = self.request_wrapper.get_eod_data_and_paging(symbol=self.symbol, index=self.current_page)

        filtered_data = [a for a in page_data if a.time > self.last_section_last_date]

        _stop_early = len(filtered_data) != len(page_data)
        self.data_for_symbol.extend(filtered_data)

        self.send_log(progress_text=f"[pages={self.current_page:>3} of {self.max_pages:>3}]")

        self.current_page = self.current_page + 1
        if self.current_page > self.max_pages or _stop_early:
            if len(filtered_data) > 0:
                self._save_data()
                self.is_done = True
                self.send_log(progress_text="SAVED")
            else:
                self.is_done = True
                self.send_log(progress_text="NO DATA")


    def _save_data(self):
        sorted_records = sorted(self.data_for_symbol, key=lambda x: x.time)

        grouped_by_year = {
            year: list(group)
            for year, group in groupby(sorted_records, key=lambda x: x.time.year)
        }

        for year, records in grouped_by_year.items():
            object_name = _get_object_name(year=year, symbol=self.symbol)
            data_to_save = [a.to_dict() for a in records]
            self.minio.put_json(self.bucket_name, object_name, data_to_save)

        meta_object_name = _get_meta_object_name(symbol=self.symbol)
        last_refresh_time = datetime.now(timezone.utc).date().isoformat()
        self.minio.put_json(self.bucket_name, meta_object_name, dict(last_refresh_time=last_refresh_time))

    def _load_last_group_of_data(self):
        prefix = _get_dir_prefix(self.symbol)
        objects = self.minio.list_objects(self.bucket_name, prefix=prefix, recursive=False)
        objects = [a.object_name for a in objects]
        objects = [a.removeprefix(prefix).removesuffix(".json") for a in objects]
        objects = [a for a in objects if a != "meta"]
        objects = [int(a.split("_")[0]) for a in objects]
        if len(objects) == 0:
            return
        last_year = max(objects)
        object_name = _get_object_name(year=last_year, symbol=self.symbol)
        last_section_of_data = self.minio.get_json(self.bucket_name, object_name)
        self.last_section_last_date = max([datetime.fromisoformat(a["time"]).date() for a in last_section_of_data])
        self.data_for_symbol = [BiznesEodRecord(DictObj(a), self.symbol) for a in last_section_of_data]

    def get_last_refresh_time(self):
        meta_object_name = _get_meta_object_name(symbol=self.symbol)
        data = self.minio.get_json(self.bucket_name, meta_object_name)
        if data is None:
            return datetime.fromisoformat('1900-01-01').date()
        return datetime.fromisoformat(data['last_refresh_time']).date()
