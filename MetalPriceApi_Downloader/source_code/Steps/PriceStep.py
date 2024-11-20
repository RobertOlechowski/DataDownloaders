from datetime import datetime, timezone, timedelta

from source_code.Steps.BaseStep import BaseStep

class PriceStep(BaseStep):
    def __init__(self):
        super().__init__()

        self.tickers = None
        self.query_time = None

    def _get_last_date(self):
        bucket_name = self.config.app.bucket_name

        files = self.minio.client.list_objects(bucket_name, prefix="rates/", recursive=False)
        years = [a.object_name.removeprefix("rates/").removesuffix("/") for a in files]
        if len(years) == 0:
            return datetime.fromisoformat('1992-01-01').date()

        max_year = max([int(a) for a in years])
        _prefix = f"rates/{max_year}/"
        files = self.minio.client.list_objects(bucket_name, prefix=_prefix, recursive=False)
        dates = [a.object_name.removeprefix(_prefix).removesuffix(".json") for a in files]
        if len(dates) == 0:
            return datetime.fromisoformat('1992-01-01').date()

        return (max([datetime.fromisoformat(a) for a in dates]) + timedelta(days=1)).date()

    def _process_tickers(self):
        symbols = self.request_wrapper.get_symbols()
        self.tickers = [a["symbol"] for a in symbols]

        time = datetime.now(timezone.utc)
        symbols_object_name = f"symbols/{time.year}_{time.month:02}.json"

        self.minio.put_json(self.config.app.bucket_name, symbols_object_name, symbols)
        self.send_log(name="Symbols", is_done=True, progress=len(self.tickers))

    def process(self):
        #self.rate_limiter.call_wait()

        if self.query_time is None:
            self.query_time = self._get_last_date()

        if self.tickers is None:
            self._process_tickers()

        data = self.request_wrapper.get_data(tickers=self.tickers, query_time=self.query_time)
        year = data["timestamp"].year
        time_text = data["timestamp"].date().isoformat()
        object_name = f"rates/{year}/{time_text}.json"

        data["timestamp"] = time_text
        self.minio.put_json(self.config.app.bucket_name, object_name, data)

        self.send_log(name="Price", sub_name=time_text, is_done=True, progress=len(data["rates"]))

        stop_date = datetime.now(timezone.utc).date()

        if (stop_date - self.query_time).days < 3:
            self.is_done = True
            return

        self.query_time = self.query_time + timedelta(days=1)


