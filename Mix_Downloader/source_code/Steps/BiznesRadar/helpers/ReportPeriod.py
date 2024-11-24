import re
import dateparser

from source_code.helpers.other import get_list


class ReportPeriod:
    def __init__(self, mode, _index, html_node):
        self.raw_data = [a.text.strip() for a in html_node.contents]
        self.index = _index

        self.year = None
        self.period_id = None
        self.time_stamp = None
        self.time_id = None

        try:
            self._parse_self(mode)
            self.is_valid = True
        except:
            self.is_valid = False

    def _parse_self(self, mode):
        row_1 = self.raw_data[0].split("/")

        if len(row_1) not in [1, 2]:
            raise Exception("Error date column")

        self.year = int(row_1[0])
        self.period_id = get_list(row_1, 1, "Y")

        if mode == "Y" and self.period_id != "Y":
            raise Exception("Error date column")

        if mode == "Q" and self.period_id not in ["Q1", "Q2", "Q3", "Q4"]:
            raise Exception("Error date column")

        row_2 = self.raw_data[1]
        row_2 = re.sub('[()]', "", row_2)

        self.time_stamp = dateparser.parse(f"{self.year} {row_2}", languages=['pl'])
        self.time_id = f"{self.time_stamp:%Y}:{self.period_id}"