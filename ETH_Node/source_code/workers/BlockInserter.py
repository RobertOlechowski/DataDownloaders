import pickle

from sqlalchemy.orm import sessionmaker

from source_code.helpers.ProgresLog import ProgresLog
from source_code.workers.BaseWorker import BaseWorker


class BlockInserter(BaseWorker):
    def __init__(self, index, stop_event):
        super().__init__(stop_event, "block_inserter", index)

        self.redis = self.config.get_redis()
        self.engine = self.config.get_db_engine()

        self.session_maker = sessionmaker(bind=self.engine)
        self.session = self.session_maker()

    def init(self):
        pass

    def step(self):
        _block_serialized = self.redis.lpop("block_data")
        if _block_serialized is None:
            self._wait_sleep()
            return

        _block = pickle.loads(_block_serialized)
        self.session.add(_block)
        self.session.commit()

        self.redis.rpush("log", pickle.dumps(ProgresLog(name=self.name, height=_block.id)))

