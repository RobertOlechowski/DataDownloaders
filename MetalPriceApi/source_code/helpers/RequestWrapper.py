import sys
import threading
import time
import traceback

import requests

from source_code.helpers.DictObj import DictObj

_local_sessions = threading.local()


def _get_session():
    if not hasattr(_local_sessions, "session"):
        _local_sessions.session = (requests.Session(),)

    session, = _local_sessions.session
    return session


def _make_request_impl(config, request_data):
    headers = {'content-type': 'application/json',
               'Accept-Encoding': 'gzip, deflate',
               'Accept': '*/*',
               'Connection': 'keep-alive',
               'X-CMC_PRO_API_KEY': config.coinmarketcap.api_key
               }

    _session = _get_session()

    response = _session.get(url=request_data.endpoint, params=request_data.params, headers=headers)

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

    if response.status.error_code != 0:
        raise Exception(f"error_code is {response.status.error_code}")

    return json_data["data"]


def _make_request(config, request_data):
    for sleep_time in [1, 5, 60]:
        try:
            return _make_request_impl(config, request_data)
        except Exception as e:
            print(f"Error [{repr(e)}] and sleep for {sleep_time}", file=sys.stderr)
            traceback.print_exc()
            time.sleep(sleep_time)
    raise e


class RequestWrapper:
    def __init__(self, config):
        self.config = config

    def get_data(self, request_data):
        data = _make_request(self.config, request_data)
        return data
