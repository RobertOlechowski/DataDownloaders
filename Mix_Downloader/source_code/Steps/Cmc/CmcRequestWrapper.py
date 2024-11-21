import sys
import threading
import time
import traceback

import requests
from ROTools.Helpers.DictObj import DictObj
from ROTools.Helpers.RequestHelper import get_session_data


def _make_request_impl(endpoint, params, api_key):
    headers = {'content-type': 'application/json',
               'Accept-Encoding': 'gzip, deflate',
               'Accept': '*/*',
               'Connection': 'keep-alive',
               'X-CMC_PRO_API_KEY': api_key
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
    response = DictObj(json_data)

    if response.status.error_code != 0:
        raise Exception(f"error_code is {response.status.error_code}")

    return json_data["data"]


def _make_request(**all_params):
    for sleep_time in [1, 5, 15, 25, 45, 60, 90]:
        try:
            return _make_request_impl(**all_params)
        except Exception as e:
            print(f"Error [{repr(e)}] and sleep for {sleep_time}", file=sys.stderr)
            traceback.print_exc()
            time.sleep(sleep_time)
    raise e


class CmcRequestWrapper:
    def __init__(self, step_config):
        self.step_config = step_config

    def get_data(self, endpoint, params):
        data = _make_request(endpoint=endpoint, params=params, api_key=self.step_config.api_key)
        return data
