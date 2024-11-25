from datetime import datetime

class SubReport:
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