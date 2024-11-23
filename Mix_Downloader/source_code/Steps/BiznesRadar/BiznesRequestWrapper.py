import hashlib
import sys
import threading
import time
import traceback
from datetime import datetime

import dateparser
import requests
from ROTools.Helpers.DictObj import DictObj
from ROTools.Helpers.RequestHelper import get_session_data, make_request_wrapper


def _make_request_impl(endpoint):
    from bs4 import BeautifulSoup
    http_response = requests.get(endpoint, headers={'Cache-Control': 'no-cache'}, timeout=10)

    content = http_response.headers['Content-Type']
    if content not in {'text/html; charset=UTF-8'}:
        raise Exception(f"content type: {http_response.headers['content-type']}")

    if http_response.status_code != 200:
        print(f"URL: {http_response.request.url}")
        raise Exception(f"status code: {http_response.status_code}")

    if http_response.encoding.lower() != "utf-8":
        raise Exception(f"encoding: {http_response.encoding}")

    soup = BeautifulSoup(http_response.content, 'lxml')
    return soup


def _build_recommendation_record(row):
    row_list = [b.text.strip() for b in row.select("tr td")]
    name, _type, target_price, _, _, _, date_text, author, _ = row_list

    _first_link = row.select_one("td:first-child a")['href']
    link = row.select_one("td:last-child a")['href']

    ticker = _first_link.split("/")[-1]

    pub_date = dateparser.parse(date_text, languages=['pl']).isoformat()

    target_price = None if target_price == "-" else float(target_price.replace(",", ".").replace(" ", ""))
    file_name = hashlib.sha256(link.encode()).hexdigest()[:20] + ".pdf"

    return dict(ticker=ticker, type=_type, pub_date=pub_date, author=author, target_price=target_price, link=link, file_name=file_name)


class BiznesRequestWrapper:
    def __init__(self, step_config, rate_limiter):
        self.step_config = step_config
        self.rate_limiter = rate_limiter

    def _get_and_select_data(self, endpoint, selector):
        raw_data = make_request_wrapper(_make_request_impl, endpoint=endpoint)
        table_data = raw_data.select(selector)

        if len(table_data) != 1:
            raise Exception("Parse error")

        return table_data[0]


    def get_symbols(self, symbol_type):
        self.rate_limiter.call_wait()

        _url_lut = {
            "GPW": "https://www.biznesradar.pl/gielda/akcje_gpw",
            "NewConnect": "https://www.biznesradar.pl/gielda/newconnect",
            "CFD": "https://www.biznesradar.pl/gielda/towary",
            "Index": "https://www.biznesradar.pl/gielda/indeksy",
        }

        table = self._get_and_select_data(endpoint=_url_lut[symbol_type], selector="main .overflow-scroll table")
        data = table.select("tr > td:first-child > a")
        if len(data) < 10:
            raise Exception("Parse error")

        data = [dict(type=symbol_type, ticker=a.text.split(" ")[0], name=a['href'].split("/")[-1], text=a.text) for a in data]
        return data

    def get_recommendations(self):
        self.rate_limiter.call_wait()
        url = "https://www.biznesradar.pl/rekomendacje/"
        table = self._get_and_select_data(endpoint=url, selector="main .overflow-scroll table")

        table_header = table.select_one("tr:has(th)").select("th")
        text_header = [a.text for a in table_header]
        if len(text_header) != 9 or text_header[0] != "Profil" or text_header[8] != "Plik":
            raise Exception("Parse error")

        data_rows = table.select("tr:not(.ad):not(:has(th))")
        data = [_build_recommendation_record(a) for a in data_rows]

        return data
