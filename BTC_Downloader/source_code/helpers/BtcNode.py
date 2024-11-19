import json
import sys
import threading
import time
import requests
from ROTools.Helpers.DictObj import DictObj

_local_sessions = threading.local()

def _get_session_data():
    if not hasattr(_local_sessions, "session"):
        _local_sessions.session = (requests.Session(), 0)

    session, counter = _local_sessions.session
    counter += 1
    _local_sessions.session = (session, counter)

    return session, counter


def _make_request_impl(config, method, params):
    headers = {'content-type': 'application/json',
               'Accept-Encoding': 'gzip, deflate',
               'Accept': '*/*',
               'Connection': 'keep-alive',
               }
    _auth = (config.node.user, config.node.password)

    _session, counter = _get_session_data()

    payload = dict(method=method, params=params, jsonrpc="2.0", id=counter)

    response = _session.post(config.node.endpoint, data=json.dumps(payload), headers=headers, auth=_auth, timeout=30)
    if response.status_code != 200:
        raise Exception(f"Response code is {response.status_code}")

    response_json = response.json()
    if response_json["id"] != counter:
        raise Exception("ID ERROR")

    if response_json["error"] is not None:
        raise Exception("error ERROR")

    return response_json


def _make_request(config, method, params):
    for sleep_time in [1, 5, 60]:
        try:
            return _make_request_impl(config, method, params)
        except Exception as e:
            print(f"Error [{repr(e)}] and sleep for {sleep_time}", file=sys.stderr)
            time.sleep(sleep_time)
    raise e


class BtcNode:
    def __init__(self, config):
        self.config = config

    def get_latest_block_number(self):
        _json = _make_request(self.config, "getblockcount", [])
        return DictObj(_json).result

    def get_block_hash(self, block_height):
        if block_height is None:
            raise Exception("block_height must be specified")
        _json = _make_request(self.config, "getblockhash", [block_height])
        return DictObj(_json).result

    def get_block_by_hash(self, block_hash=None):
        _json = _make_request(self.config, "getblock", [block_hash, 2])
        _json = _json["result"]
        return _json

    def get_block_by_height(self, block_height):
        _block_hash = self.get_block_hash(block_height)
        return self.get_block_by_hash(block_hash=_block_hash)
