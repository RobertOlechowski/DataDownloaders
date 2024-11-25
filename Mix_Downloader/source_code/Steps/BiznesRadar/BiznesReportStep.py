import itertools
from datetime import datetime, timezone

from source_code.Steps.BaseStep import BaseStep
from source_code.Steps.BiznesRadar.helpers.FullReport import merge_reports


def _get_object_name(symbol, year, period_name):
    return f"report/{symbol.ticker}/{symbol.ticker}_{year}_{period_name}.json"

def _get_meta_object_name(symbol):
    return f"report/{symbol.ticker}/meta.json"


class BiznesReportStep(BaseStep):
    def __init__(self, config, step_config, symbol=None, request_wrapper=None):
        super().__init__(config, step_config)

        self.request_wrapper = request_wrapper

        self.name = "Biznes"
        self.sub_name = f"Report {symbol.ticker}"
        self.symbol = symbol

        self.pages = [*itertools.product(["zysk_strata", "bilans", "przeplyw"], ["Q", "Y"]), ("ws_wartosci", "Q")]
        self.all_reports = []

    def init_impl(self):
        last_date = self.get_last_refresh_time()

        days_delta = (datetime.now(timezone.utc).date() - last_date).days
        if days_delta <= self.step_config.refresh_threshold_days:
            self.is_done = True
            self.send_log(is_skipped=True)
            return

        self.send_log(is_started=True)


    def process(self):
        if len(self.pages) > 0:
            page_code, mode = self.pages.pop(0)
            reports = self.request_wrapper.get_report_data(symbol=self.symbol, mode=mode, page_code=page_code)
            self.all_reports.extend(reports)
            self.send_log(progress_text=f"{mode} {page_code} {len(self.pages)}")
            return

        reports = merge_reports(self.symbol, self.all_reports)
        for item in reports:
            self._save_report(item)

        last_refresh_time = datetime.now(timezone.utc).date().isoformat()
        meta_object_name = _get_meta_object_name(symbol=self.symbol)
        self.minio.put_json(self.bucket_name, meta_object_name, dict(last_refresh_time=last_refresh_time))

        self.is_done = True
        self.send_log()

    def _save_report(self, report):
        object_name = _get_object_name(symbol=self.symbol, year=report.period.year, period_name=report.period.period_id)
        json_data = report.to_dict()

        old_data = self.minio.get_json(self.bucket_name, object_name)

        if old_data is None:
            self.minio.put_json(self.bucket_name, object_name, json_data)
            return

        if old_data == json_data:
            return

        raise Exception("Not implemented")

    def get_last_refresh_time(self):
        meta_object_name = _get_meta_object_name(symbol=self.symbol)
        data = self.minio.get_json(self.bucket_name, meta_object_name)
        if data is None:
            return datetime.fromisoformat('1900-01-01').date()
        return datetime.fromisoformat(data['last_refresh_time']).date()
