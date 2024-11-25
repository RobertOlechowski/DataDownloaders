class BaseNodeWrapper:

    def __init__(self, node_config):
        self.config = node_config
        self.type = node_config.type

        self.max_block_height = -1
        self.delta_t = None
        self.blocs_per_hour = None
        self.logs_count = None

    def process_logs(self, logs):
        self.delta_t = None
        self.blocs_per_hour = None

        logs = [a for a in logs if a.node_type == self.type]
        values = [a.height for a in logs]
        self.max_block_height = max([self.max_block_height, *values])

        self.logs_count = len(logs)

        if self.logs_count > 1:
            self.delta_t = logs[-1].get_time_delta(logs[0])
            self.blocs_per_hour = 60.0 * 60.0 * float(self.logs_count) / self.delta_t