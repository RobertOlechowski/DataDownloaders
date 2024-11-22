from datetime import datetime, timezone

import humanize


class ProgresLog:
    def __init__(self, name,
                 sub_name="",
                 progress=-1,
                 progress_text=None,
                 status=None):
        self.name = name
        self.sub_name = sub_name
        self.status = status
        self.progress = progress
        self.progress_text = progress_text
        self.time = datetime.now(timezone.utc)
        self.tags = set([])

    def add_tag(self, tag):
        self.tags.add(tag)

    def del_tag(self, tag):
        self.tags.discard(tag)

    def check_tag(self, tag):
        return tag in self.tags

    def is_in_progress(self):
        return not self.is_ended()

    def is_ended(self):
        return self.status in ["DONE, SKIP"]

    def get_log(self):
        _time_ago = humanize.naturaltime(datetime.now(timezone.utc) - self.time)

        progress_msg = self.progress_text
        progress_msg = progress_msg or f"{self.progress:>5}" or "---"

        return f"  ---[{self.name:>12}] [{self.sub_name:<28}] {self.status:<10} --> {progress_msg}   [{_time_ago}]"
