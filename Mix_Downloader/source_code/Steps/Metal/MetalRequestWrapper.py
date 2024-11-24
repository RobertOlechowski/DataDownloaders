from datetime import datetime

import pytz
from ROTools.Helpers.DictObj import DictObj
from ROTools.Helpers.RequestHelper import get_session_data, make_request_wrapper


def _make_request_impl(endpoint, params):
    headers = {'content-type': 'application/json',
               'Accept-Encoding': 'gzip, deflate',
               'Accept': '*/*',
               'Connection': 'keep-alive',
               }

    _session, _ = get_session_data()
    response = _session.get(url=endpoint, params=params, headers=headers)

    if response.status_code != 200:
        msg = None
        try:
            value = response.json()
            value = value["status"]
            msg = value["error_message"]
        except:
            pass

        if msg is None:
            raise Exception(f"Response code is {response.status_code}  url = {response.request.url}")
        raise Exception(f"Error {msg}  url = {response.request.url}")

    json_data = response.json()
    response2 = DictObj(json_data)
    if not response2.success:
        raise Exception(f"Success is FALSE")
    return response2


class MetalRequestWrapper:
    def __init__(self, step_config):
        self.task_config = step_config

    def get_symbols(self):
        endpoint = "https://api.metalpriceapi.com/v1/symbols"
        params = {
            'api_key': self.task_config.api_key,
        }

        _data = make_request_wrapper(_make_request_impl, endpoint=endpoint, params=params)
        keys = _data.symbols.__dict__.keys()
        _data = [(a, getattr(_data.symbols, a)) for a in keys]
        _data = [dict(symbol=a, name=b) for a, b in _data]
        return _data

    def get_data(self, tickers, query_time):

        _query_time = query_time.strftime("%Y-%m-%d")

        endpoint = f"https://api.metalpriceapi.com/v1/{_query_time}"
        params = {
            'api_key': self.task_config.api_key,
            'base': "USD",
            'currencies': ",".join(tickers)
        }
        tickers = set(tickers)
        data = make_request_wrapper(_make_request_impl, endpoint=endpoint, params=params)

        rates = data.rates
        rates = [(k, v) for k, v in rates.items() if k in tickers]
        timestamp = datetime.fromtimestamp(data.timestamp, pytz.utc)

        return dict(timestamp=timestamp, rates=rates)
