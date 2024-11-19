from datetime import datetime, timezone

from dateutil import parser
from dateutil.relativedelta import relativedelta

from source_code.Steps.BaseStep import BaseStep

_meta_date_tag = 'x-amz-meta-last_record'


class OHLCVStep(BaseStep):
    def __init__(self, symbol=None, interval=None):
        super().__init__()

        if interval not in ["daily", "hourly"]:
            raise Exception("Invalid interval. Valid values: daily, hourly")

        self.symbol = symbol
        self.id_number = symbol.id
        self.interval = interval
        self.main_name = "OHLCV"
        self.sub_name = symbol.symbol

        self.params = dict(id=symbol.id, interval=interval, time_period=interval, count=5000)
        self.endpoint = "https://pro-api.coinmarketcap.com/v2/cryptocurrency/ohlcv/historical"

        self.counter = 0
        self._start_date = None
        self.data_wrapper = None
        self.data_records = []

    def _get_object_symbol_mask(self):
        return f"{self.interval}/{self.symbol.id:06}_{self.symbol.symbol}"

    def get_last_record_date(self):
        mask = self._get_object_symbol_mask()
        files = self.minio.client.list_objects(self.bucket_name, prefix=mask, recursive=True)
        files = list(files)
        meta_data = [dict(self.minio.get_metadata(self.bucket_name, a.object_name)) for a in files]
        dates = [a[_meta_date_tag] for a in meta_data]
        dates = [parser.parse(a) for a in dates]
        last_date = max(dates) if len(dates) > 0 else None
        if self.interval == "daily":
            last_date = last_date.date() if last_date is not None else None
        return last_date

    def _get_plan_start_date(self):
        if self.interval == "daily":
            return datetime.now(timezone.utc).date() + relativedelta(months=-self.config.app.daily_range)
        if self.interval == "hourly":
            return datetime.now(timezone.utc) + relativedelta(months=-self.config.app.hourly_range)
        raise Exception("FLOW")

    def _get_text_start_date(self):
        format_str = '%Y-%m-%d' if self.interval == "daily" else '%Y-%m-%dT%H:%M:%S'
        return self._start_date.strftime(format_str)

    def _convert_date(self, date):
        return date.date() if self.interval == "daily" else date

    def _parse_record_date(self, data_record):
        record_date = parser.parse(data_record['time_open'])
        return self._convert_date(record_date)

    def _get_object_name(self, file_key):
        _mask = self._get_object_symbol_mask()
        if self.interval == "daily":
            return f"{_mask}/{file_key}.json"
        if self.interval == "hourly":
            year, month = file_key
            return f"{_mask}/{year}/{month:02}_{year}.json"
        raise Exception("FLOW")

    def _get_date_key(self, date):
        if self.interval == "daily":
            return date.year
        if self.interval == "hourly":
            return date.year, date.month
        raise Exception("FLOW")

    def init_impl(self):
        _start_date = self._get_plan_start_date()
        last_date = self.get_last_record_date()
        self._start_date = max(_start_date, last_date or _start_date)

        days_delta = (self._convert_date(datetime.now(timezone.utc)) - self._start_date).days
        if days_delta <= 1:
            self.is_done = True
            self.send_log(progress_text=f"{self._start_date} [{-1:>3}]", skip=True)
            return

        self.send_log(progress_text=f"????? [{self.counter:>3}]")

    def _get_data(self):
        self.params["time_start"] = self._get_text_start_date()
        self.counter += 1
        data = self.request_wrapper.get_data(request_data=self)
        data_quotes = data['quotes']

        self.data_records.extend(data_quotes)
        if self.data_wrapper is None:
            self.data_wrapper = data
            self.data_wrapper['quotes'] = []

        if len(data_quotes) > 0:
            self._start_date = self._parse_record_date(data_quotes[-1])
            return

        self.is_done = True

    def save_data(self):
        parse_cb = lambda a: parser.parse(a['time_open']).date()
        data = [(parse_cb(a), a) for a in self.data_records]

        data = [(self._get_date_key(a), item) for a, item in data]
        keys = list(set([a for a, _ in data]))
        keys.sort()

        for file_key_loop in keys:
            object_name = self._get_object_name(file_key_loop)
            records = [c for file_key, c in data if file_key == file_key_loop]
            last_date = max([self._parse_record_date(a) for a in records])

            if self.minio.object_exists(self.bucket_name, object_name):
                json_data = self.minio.get_json(self.bucket_name, object_name)
            else:
                json_data = dict(self.data_wrapper)

            json_data['quotes'].extend(records)

            # detect date duplications
            from collections import Counter
            dates = [parser.parse(a['time_open']) for a in json_data['quotes']]
            counter = Counter(dates)
            duplicates = [item for item, count in counter.items() if count > 1]
            if len(duplicates) > 0:
                raise Exception("Duplicated dates")

            self.minio.put_json(self.bucket_name, object_name, json_data, metadata={_meta_date_tag: last_date.isoformat()})

    def process(self):
        self.rate_limiter.call_wait()
        self._get_data()

        if not self.is_done:
            self.send_log(progress_text=f"{self.params['time_start']} [{self.counter:>3}]")
            return

        if len(self.data_records) > 0:
            self.save_data()

        self.is_done = True
        self.send_log(progress_text=f"{self.params['time_start']} [{self.counter:>3}]")
