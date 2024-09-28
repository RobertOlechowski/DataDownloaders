import json
import sys
import threading
import time
import traceback

import requests

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

    _session, counter = _get_session_data()

    payload = dict(method=method, params=params, jsonrpc="2.0", id=counter)

    response = _session.post(config.node.endpoint, data=json.dumps(payload), headers=headers, auth=None, timeout=15)
    response.raise_for_status()

    response_json = response.json()
    if response_json["id"] != counter:
        raise Exception("ID ERROR")

    _error = response_json.get("error", None)
    if _error is not None:
        _message = _error["message"]
        raise Exception(f"Error msg: {_message}")

    return response_json["result"]


def _make_request(config, method, params):
    for sleep_time in [1, 5, 60]:
        try:
            return _make_request_impl(config, method, params)
        except Exception as e:
            print(f"Error [{repr(e)}] and sleep for {sleep_time}", file=sys.stderr)
            traceback.print_exc()
            time.sleep(sleep_time)
    raise e


class EthNode:
    def __init__(self, config):
        self.config = config

    def get_latest_block_number(self):
        _json = _make_request(self.config, "eth_blockNumber", [])
        _number = int(_json, 16)
        return _number

    def get_block_by_height(self, block_height):
        block_number = f"0x{block_height:x}"
        response_block = _make_request(self.config, "eth_getBlockByNumber", params=[block_number, True])

        if response_block["number"] != block_number:
            raise Exception("Invalid block height")

        response_receipts = _make_request(self.config, "eth_getBlockReceipts", params=[block_number])
        receipts = {a["transactionHash"]: a for a in response_receipts}

        for tx in response_block["transactions"]:
            _tx_hash = tx["hash"]
            tx["receipt"] = receipts[_tx_hash]

        uncles = response_block.get("uncles", [])
        uncle_cb = lambda a: _make_request(self.config, "eth_getUncleByBlockNumberAndIndex", params=[block_number, f"0x{a:x}"])
        response_block["uncles"] = [uncle_cb(a) for a in range(len(uncles))]

        return response_block
