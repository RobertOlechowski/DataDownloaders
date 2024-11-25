from collections import defaultdict
from datetime import datetime
from itertools import chain, groupby

def merge_reports(symbol, reports):
    grouped = defaultdict(list)
    for item in reports:
        grouped[item.period.time_id].append(item)

    result = []
    for period_text, items in grouped.items():
        if len(items) < 3:
            continue

        period = items[0].period
        records = list(chain.from_iterable([a.records for a in items]))
        result.append(FullReport(symbol, period, records))
    return result


class FullReport:
    def __init__(self, symbol, period, records):
        self.period = period
        self.symbol = symbol

        grouped = defaultdict(list)
        for k, name, value in records:
            grouped[k].append((k, name, value))

        primary_report = grouped.pop('PrimaryReport')
        if len(set(primary_report)) != 1:
            raise Exception("Primary report error")
        self.records = [primary_report[0]]

        for item in grouped.values():
            if len(set(item)) != 1:
                raise Exception("field duplication report error")
            self.records.append(item[0])

    def to_dict(self):
        records = [dict(field_id=field_id, field_name=field_name, value=value) for field_id, field_name, value in self.records]
        symbol = dict(name=self.symbol.name, type=self.symbol.type, ticker=self.symbol.ticker)

        for item in records:
            if isinstance(item["value"], datetime):
                item["value"] = item["value"].isoformat()
        return dict(symbol=symbol, period=self.period.to_dict(), records=records)