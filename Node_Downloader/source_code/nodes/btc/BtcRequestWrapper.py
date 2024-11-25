import json

from ROTools.Helpers.DictObj import DictObj
from ROTools.Helpers.RequestHelper import make_request_wrapper, get_session_data


def _make_request_impl(node_config, method, params):
    headers = {'content-type': 'application/json',
               'Accept-Encoding': 'gzip, deflate',
               'Accept': '*/*',
               'Connection': 'keep-alive',
               }
    _auth = None
    if node_config.user != "?????":
        _auth = (node_config.user, node_config.password)

    _session, counter = get_session_data()

    payload = dict(method=method, params=params, jsonrpc="2.0", id=counter)

    response = _session.post(node_config.endpoint, data=json.dumps(payload), headers=headers, auth=_auth, timeout=20)
    response.raise_for_status()

    response_json = response.json()
    if response_json["id"] != counter:
        raise Exception("ID ERROR")

    if response_json["error"] is not None:
        raise Exception("error ERROR")

    return response_json



_sleep_times=(1, 5, 10, 15, None)

class BtcRequestWrapper:
    def __init__(self, config):
        self.node_config = config

    def get_latest_block_number(self):
        _json = make_request_wrapper(_make_request_impl, sleep_times=_sleep_times, node_config=self.node_config, method="getblockcount", params=[])
        return DictObj(_json).result

    def get_block_hash(self, block_height):
        if block_height is None:
            raise Exception("block_height must be specified")
        _json = make_request_wrapper(_make_request_impl, sleep_times=_sleep_times, node_config=self.node_config, method="getblockhash", params=[block_height])
        return DictObj(_json).result

    def get_block_by_hash(self, block_hash=None):
        _json = make_request_wrapper(_make_request_impl, sleep_times=_sleep_times, node_config=self.node_config, method="getblock", params=[block_hash, 2])
        _json = _json["result"]
        return _json

    def get_block_by_height(self, block_height):
        _block_hash = self.get_block_hash(block_height)
        return self.get_block_by_hash(block_hash=_block_hash)
