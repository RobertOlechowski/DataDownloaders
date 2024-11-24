from datetime import datetime

class Report:
    def __init__(self, period, data, report_fields, page):
        self.period = period
        self.report_key = f"{page}___{period.time_id}"

        data = [(field_id, report_fields[field_id], self._process_cell(field_id, cell_data)) for field_id, cell_data in data]
        self.records = [(field_id, field_text, field_value) for field_id, field_text, field_value in data if field_value is not None]

    def _process_cell(self, field_id, cell_data):
        if cell_data is None:
            return None
        cell_data = cell_data.strip().replace(" ", "").replace(",", ".")

        if field_id in ["PrimaryReport"]:
            return datetime.strptime(cell_data, "%Y-%m-%d")

        if "." in cell_data:
            return float(cell_data)

        return int(cell_data)

    def __repr__(self):
        return f"{self.report_key}"


class ReportFieldBuilder:
    def __init__(self, mode, report_fields):
        self.mode = mode
        self.report_fields = {a: b for a, b in report_fields}

    def build_report(self, report_period, html_data, page):
        column_index = report_period.index
        raw_data = [(head, data[column_index].select_one("span.value .pv, span.value .premium-value span")) for head, data in html_data]
        raw_data = [(head, data)for head, data in raw_data if data is not None]
        raw_data = [(head, data.text) for head, data in raw_data]

        return Report(report_period, data=raw_data, report_fields=self.report_fields, page=page)