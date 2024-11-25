import json

from ROTools.Helpers.RequestHelper import get_session_data, make_request_wrapper

_headers = {'content-type': 'application/json',
           'Accept-Encoding': 'gzip, deflate',
           'Accept': '*/*',
           'Connection': 'keep-alive',
           }

def _make_request_impl(node_config, method, params):
    _session, counter = get_session_data()

    payload = dict(method=method, params=params, jsonrpc="2.0", id=counter)

    response = _session.post(node_config.endpoint, data=json.dumps(payload), headers=_headers, auth=None, timeout=20)
    response.raise_for_status()

    response_json = response.json()
    if response_json["id"] != counter:
        raise Exception("ID ERROR")

    _error = response_json.get("error", None)
    if _error is not None:
        _message = _error["message"]
        raise Exception(f"Error msg: {_message}")

    return response_json["result"]

_sleep_times=(1, 5, 10, 15, 25, None)

class EthRequestWrapper:
    def __init__(self, config):
        self.node_config = config

    def make_request(self, method, params):
        return make_request_wrapper(_make_request_impl, sleep_times=_sleep_times, node_config=self.node_config, method=method, params=params)

    def get_latest_block_number(self):
        _json = self.make_request(method="eth_blockNumber", params=[])
        _number = int(_json, 16)
        return _number

    def get_block_by_height(self, block_height):
        block_number = f"0x{block_height:x}"
        response_block = self.make_request(method="eth_getBlockByNumber", params=[block_number, True])

        if response_block["number"] != block_number:
            raise Exception("Invalid block height")

        response_receipts = self.make_request(method="eth_getBlockReceipts", params=[block_number])
        receipts = {a["transactionHash"]: a for a in response_receipts}

        for tx in response_block["transactions"]:
            _tx_hash = tx["hash"]
            tx["receipt"] = receipts[_tx_hash]

        uncles = response_block.get("uncles", [])
        uncle_cb = lambda a: self.make_request( method="eth_getUncleByBlockNumberAndIndex", params=[block_number, f"0x{a:x}"])
        response_block["uncles"] = [uncle_cb(a) for a in range(len(uncles))]

        return response_block