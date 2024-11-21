from datetime import datetime

import pytz
from ROTools.Helpers.DictObj import DictObj
from bs4 import BeautifulSoup

from source_code.helpers.other import is_equal, parse_float


class FarsideRequestWrapper:
    def __init__(self, step_config):
        self.task_config = step_config

    def _parse_value(self, text):
        if text=="-":
            return None
        value = text.replace("(", "-").replace(")", "").replace(",", "")
        return parse_float(value, error_value=text)

    def _parse_row(self, row_data, columns):
        if len(row_data) <= 1:
            return None
        row_head = row_data[0].text

        if row_head in ["Total", "Average", "Maximum", "Minimum"]:
            return None

        try:
            row_head = datetime.strptime(row_head, "%d %b %Y")
            row_head = row_head.replace(tzinfo=pytz.utc).date().isoformat()
        except:
            pass

        value_cells = [a.text for a in row_data[1:]]
        value_cells = [self._parse_value(a) for a in value_cells]

        to_sum = [a for a in value_cells if isinstance(a, float)]
        value_sum = sum([a or 0.0 for a in to_sum][:-1])

        if not is_equal(value_sum, value_cells[-1], epsilon=0.001) and row_head not in ["Seed"]:
            raise Exception(f"Parse error sum {value_sum} != {value_cells[-1]}")

        if len(columns) != len(value_cells):
            raise Exception("Parse error")

        values = [dict(index=col["index"], key=col["key"], value=a) for a, col in zip(value_cells, columns)]
        values = [a for a in values if a["value"] is not None]

        return row_head, values

    def parse_columns_btc(self, table):
        col_heads = table.select("thead tr")
        if len(col_heads) != 1:
            raise Exception("Parse error")
        col_heads = col_heads[0]
        col_heads = col_heads.select("th div span.tabletext")
        col_heads = [a.text for a in col_heads]
        col_heads = [a for a in col_heads if a not in ["Date"]]
        col_heads = [dict(index=i, name=a, key=a) for i, a in enumerate(col_heads)]
        return col_heads

    def parse_columns_eth(self, table):
        col_heads = table.select("thead tr")
        if len(col_heads) != 3:
            raise Exception("Parse error")
        col_heads = [[b.text for b in a.select("th div span")] for a in col_heads]
        col_heads = list(zip(*col_heads))
        result = [dict(index=i, name=a, code=b, fee=c, key=f"{a}_{b}") for i, (a,b,c) in enumerate(col_heads)]
        return result

    def parse_columns(self, table):
        lut = {"btc": self.parse_columns_btc, "eth": self.parse_columns_eth}
        return lut[self.task_config.type](table)

    def get_data(self):
        import cloudscraper
        scraper = cloudscraper.create_scraper()
        response = scraper.get(self.task_config.page)
        page_source = response.text
        soup = BeautifulSoup(page_source, 'lxml')

        title = soup.select(".entry-title")
        if len(title) != 1:
            raise Exception("Parse error")
        title = title[0]

        if 'ETF Flow – All Data' not in title.text:
            raise Exception("Parse error")

        table = soup.select("div.post-inner figure table.etf")
        if len(table) != 1:
            raise Exception("Parse error")
        table = table[0]

        columns = self.parse_columns(table)

        rows = table.select("tbody tr")
        rows = [self._parse_row(a.select("td"), columns=columns) for a in rows]
        rows = [a for a in rows if a is not None]

        result = []
        for row_head, values in rows:
            result.append(dict(time=row_head, records=values))
        return dict(columns=columns, data=result)

