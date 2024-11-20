import json

from ROTools.Helpers.RequestHelper import get_session_data, make_request_wrapper

_headers = {'content-type': 'application/json',
           'Accept-Encoding': 'gzip, deflate',
           'Accept': '*/*',
           'Connection': 'keep-alive',
           }

def _make_request_impl(endpoint, method, params):
    _session, counter = get_session_data()

    payload = dict(method=method, params=params, jsonrpc="2.0", id=counter)

    response = _session.post(endpoint, data=json.dumps(payload), headers=_headers, auth=None, timeout=15)
    response.raise_for_status()

    response_json = response.json()
    if response_json["id"] != counter:
        raise Exception("ID ERROR")

    _error = response_json.get("error", None)
    if _error is not None:
        _message = _error["message"]
        raise Exception(f"Error msg: {_message}")

    return response_json["result"]


class EthNode:
    def __init__(self, config):
        self.config = config
        self.endpoint = config.node.endpoint

    def get_latest_block_number(self):
        _json = make_request_wrapper(self.endpoint, "eth_blockNumber", [], _make_request_impl)
        _number = int(_json, 16)
        return _number

    def get_block_by_height(self, block_height):
        block_number = f"0x{block_height:x}"
        response_block = make_request_wrapper(self.endpoint, "eth_getBlockByNumber", [block_number, True], _make_request_impl)

        if response_block["number"] != block_number:
            raise Exception("Invalid block height")

        response_receipts = make_request_wrapper(self.endpoint, "eth_getBlockReceipts", [block_number], _make_request_impl)
        receipts = {a["transactionHash"]: a for a in response_receipts}

        for tx in response_block["transactions"]:
            _tx_hash = tx["hash"]
            tx["receipt"] = receipts[_tx_hash]

        uncles = response_block.get("uncles", [])
        uncle_cb = lambda a: make_request_wrapper(self.endpoint, "eth_getUncleByBlockNumberAndIndex", [block_number, f"0x{a:x}"], _make_request_impl)
        response_block["uncles"] = [uncle_cb(a) for a in range(len(uncles))]

        return response_block
