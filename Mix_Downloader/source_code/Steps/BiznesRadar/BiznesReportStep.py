from datetime import datetime, timezone

from source_code.Steps.BaseStep import BaseStep


#def _get_object_name(year, symbol):
#    return f"{_get_symbol_dir(symbol)}/{year}_{symbol.ticker}.json"

def _get_meta_object_name(symbol):
    return f"{_get_symbol_dir(symbol)}/meta.json"

def _get_symbol_dir(symbol):
    return f"report/{symbol.ticker}"

#def _get_dir_prefix(symbol):
#    return f"{_get_symbol_dir(symbol)}/"

class BiznesReportStep(BaseStep):
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
        last_date = self.get_last_refresh_time()

        days_delta = (datetime.now(timezone.utc).date() - last_date).days
        if days_delta <= self.step_config.refresh_threshold_days:
            self.is_done = True
            self.send_log(is_skipped=True)
            return

        self.send_log(is_started=True)


    def process(self):
        self.is_done = True
        return
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


    def get_last_refresh_time(self):
        meta_object_name = _get_meta_object_name(symbol=self.symbol)
        data = self.minio.get_json(self.bucket_name, meta_object_name)
        if data is None:
            return datetime.fromisoformat('1900-01-01').date()
        return datetime.fromisoformat(data['last_refresh_time']).date()
