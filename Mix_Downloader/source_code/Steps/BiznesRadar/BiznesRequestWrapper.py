import hashlib

import dateparser
import requests
from ROTools.Helpers.RequestHelper import make_request_wrapper

from source_code.Steps.BiznesRadar.helpers.BiznesEodRecord import BiznesEodRecord
from source_code.Steps.BiznesRadar.helpers.ReportPeriod import ReportPeriod
from source_code.Steps.BiznesRadar.helpers.ReportFieldBuilder import ReportFieldBuilder


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

    def _select_data(self, raw_data, selector, only_single_element=True):
        selected_data = raw_data.select(selector)

        if only_single_element:
            if len(selected_data) != 1:
                raise Exception("Parse error")
            return selected_data[0]

        return selected_data

    def _get_and_select_data(self, endpoint, selector, only_single_element=True):
        raw_data = make_request_wrapper(_make_request_impl, endpoint=endpoint, sleep_times=(1, 2, 5, None))
        return self._select_data(raw_data, selector, only_single_element)

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

    def get_report_data(self, symbol, mode, page_code):
        self.rate_limiter.call_wait()
        url = {
            "zysk_strata": f"https://www.biznesradar.pl/raporty-finansowe-rachunek-zyskow-i-strat/{symbol.name},{mode}",
            "bilans": f"https://www.biznesradar.pl/raporty-finansowe-bilans/{symbol.name},{mode}",
            "przeplyw": f"https://www.biznesradar.pl/raporty-finansowe-przeplywy-pieniezne/{symbol.name},{mode}",
            "ws_wartosci": f"https://www.biznesradar.pl/wskazniki-wartosci-rynkowej/{symbol.name}",
        }[page_code]

        table = self._get_and_select_data(endpoint=url, selector="main table.report-table")
        report_periods = [ReportPeriod(mode, _index, a) for _index, a in enumerate(table.select("tr:first-child th.thq"))]
        report_fields = [(a.parent['data-field'], a.text) for a in table.select("tr td.f")]

        _builder = ReportFieldBuilder(mode, report_fields)

        data_rows = table.select("tr[data-field]:has(td.h)")
        all_data = [(item["data-field"], item.select("td.h")) for item in data_rows]
        reports = [_builder.build_report(a, all_data, page_code) for a in report_periods if a.is_valid]
        #reports = [a for a in reports if a.is_valid()]
        return reports


    def get_eod_data_and_paging(self, symbol=None, index=1):
        self.rate_limiter.call_wait()
        url = f"https://www.biznesradar.pl/notowania-historyczne/{symbol.name},{index}"
        try:
            raw_data = make_request_wrapper(_make_request_impl, endpoint=url, sleep_times=(1, 2, 5, None))
            paging = self._select_data(raw_data, selector="table.qTableFull + div.buttons > *", only_single_element=False)
            table = self._select_data(raw_data, selector="main table.qTableFull")
        except Exception as e:
            raise Exception(f"ticker = [{symbol.ticker}] url = [{url}] error: {e}")

        max_page = [x.text for x in reversed(paging) if x.text.isnumeric()]
        max_page = int(max_page[0]) if len(max_page) > 0 else 1

        table = self._select_data(raw_data, selector="main table.qTableFull")

        row_header = table.select("tr th")
        if len(row_header) not in [6, 7] or row_header[0].text != "Data":
            raise Exception("Assert")

        data_row = [tuple(b.text for b in a.select("tr td")) for a in table.select("tr")]
        data_row = [a for a in data_row if len(a) in [6, 7]]
        if len(data_row) < 1:
            raise Exception("Data error")

        return max_page, [BiznesEodRecord(a, symbol) for a in data_row]



