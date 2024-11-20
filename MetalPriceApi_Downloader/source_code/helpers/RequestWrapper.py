import sys
import threading
import time
import traceback
import requests
from ROTools.Helpers.DictObj import DictObj

_local_sessions = threading.local()


def _get_session():
    if not hasattr(_local_sessions, "session"):
        _local_sessions.session = (requests.Session(),)

    session, = _local_sessions.session
    return session


def _make_request_impl(config, endpoint, params):
    headers = {'content-type': 'application/json',
               'Accept-Encoding': 'gzip, deflate',
               'Accept': '*/*',
               'Connection': 'keep-alive',
               }

    _session = _get_session()
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
    response = DictObj(json_data)
    if not response.success:
        raise Exception(f"Success is FALSE")

    return response

def _make_request(config, endpoint, params):
    for sleep_time in [1, 5, 60]:
        try:
            return _make_request_impl(config, endpoint, params)
        except Exception as e:
            print(f"Error [{repr(e)}] and sleep for {sleep_time}", file=sys.stderr)
            traceback.print_exc()
            time.sleep(sleep_time)
    raise e


class RequestWrapper:
    def __init__(self, config):
        self.config = config

    def get_symbols(self):
        endpoint = "https://api.metalpriceapi.com/v1/symbols"
        params = {
            'api_key': self.config.data_source.api_key,
        }

        _data = _make_request(self.config, endpoint, params)
        keys = _data.symbols.__dict__.keys()
        _data = [(a, getattr(_data.symbols, a)) for a in keys]
        _data = [dict(symbol=a, name=b) for a, b in _data]
        return _data

    def get_data(self, request_data):
        data = _make_request(self.config, request_data)
        return data
