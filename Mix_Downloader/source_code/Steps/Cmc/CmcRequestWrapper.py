from ROTools.Helpers.DictObj import DictObj
from ROTools.Helpers.RequestHelper import get_session_data, make_request_wrapper


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


class CmcRequestWrapper:
    def __init__(self, step_config, rate_limiter):
        self.step_config = step_config
        self.rate_limiter = rate_limiter

    def get_data(self, endpoint, params):
        self.rate_limiter.call_wait()
        data = make_request_wrapper(_make_request_impl, endpoint=endpoint, params=params, api_key=self.step_config.api_key)
        return data
