from datetime import datetime, timezone

import humanize


class ProgresLog:
    def __init__(self, name,
                 sub_name="",
                 progress=-1,
                 progress_text=None,
                 skip=None,
                 is_done=False,
                 id_number=None,
                 interval=None):
        self.name = name
        self.sub_name = sub_name
        self.status = "Done" if is_done else "Active"
        self.is_done = is_done
        self.id_number = id_number
        self.interval = interval
        self.progress = progress
        self.is_skip = skip
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
        return not self.is_done and not self.is_skip

    def is_ended(self):
        return self.is_done or self.is_skip

    def get_log(self):
        _is_done = "IN PROGRESS"
        if self.is_done:
            _time_ago = humanize.naturaltime(datetime.now(timezone.utc) - self.time)
            _is_done = F"DONE  [{_time_ago}]"

        if self.is_skip:
            _time_ago = humanize.naturaltime(datetime.now(timezone.utc) - self.time)
            _is_done = F"SKIP  [{_time_ago}]"

        id_text = "" if self.id_number is None else f"[{self.id_number: 06}] "
        interval_text = "" if self.interval is None else f"[{self.interval:>8}] "

        progress_msg = self.progress_text
        progress_msg = progress_msg or f"{self.progress:>5}"

        return f"  ---[{self.name:>12}] {id_text} {interval_text}[{self.sub_name:<28}] --> {progress_msg}   {_is_done}"
