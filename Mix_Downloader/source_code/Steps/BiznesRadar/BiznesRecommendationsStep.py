from datetime import datetime, timezone
import os

import requests

from source_code.Steps.BaseStep import BaseStep

def _get_object_name():
    time = datetime.now(timezone.utc)
    return f"recommendations/{time.year}_{time.month:02}.json"


class BiznesRecommendationsStep(BaseStep):
    def __init__(self, config, step_config, request_wrapper=None):
        super().__init__(config, step_config)

        self.request_wrapper = request_wrapper

        self.name = "Biznes"
        self.sub_name = "Recommendations"
        self.object_name = _get_object_name()

    def init_impl(self):
        if self.minio.object_exists(self.bucket_name, self.object_name):
            self.is_done = True
            self.send_log(phase=self.sub_name, is_skipped=True)
            return

        self.send_log(phase=self.sub_name, is_started=True)

    def process(self):
        data = self.request_wrapper.get_recommendations()

        for i, item in enumerate(data):
            object_name = f"recommendations/files/{item['file_name']}"
            if self.minio.object_exists(self.bucket_name, object_name):
                continue

            url = "https://www.biznesradar.pl" + item["link"]
            response = requests.get(url)
            response.raise_for_status()
            content = response.content

            self.minio.put_object(self.bucket_name, object_name, content, content_type="application/pdf")
            self.send_log(phase=self.sub_name, progress=len(data) - i - 1)

        self.minio.put_json(self.bucket_name, self.object_name, dict(insert_date=datetime.now().isoformat(), data=data))
        self.is_done = True
        self.send_log(phase=self.sub_name, progress=len(data))
