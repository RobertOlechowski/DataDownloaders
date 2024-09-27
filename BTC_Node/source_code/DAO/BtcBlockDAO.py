from sqlalchemy import Column, Integer, String, DateTime, BIGINT, SMALLINT, REAL, LargeBinary
from sqlalchemy.orm import relationship

from source_code.DAO.BaseDAO import BaseDAO
from source_code.DAO.BtcTransactionDAO import BtcTransactionDAO
from source_code.Mapper.MapItem import MapItem
from source_code.Mapper.Mapper import Mapper

_json_mapper = Mapper(table_name=None)
_json_mapper.add(MapItem("height", _type=int, none_exception=True))

_json_mapper.add(MapItem("hash", _type="hex_to_bytes", none_exception=True))

_json_mapper.add(MapItem("version", _type=int, none_exception=True))

_json_mapper.add(MapItem("block_time", json_name="time", _type="data_s_utc", none_exception=True))
_json_mapper.add(MapItem("median_time", json_name="mediantime", _type="data_s_utc", none_exception=True))

_json_mapper.add(MapItem("nonce", _type=str, none_exception=True))
_json_mapper.add(MapItem("bits", _type="hex_to_bytes", none_exception=True))

_json_mapper.add(MapItem("difficulty", _type=int, none_exception=True))
_json_mapper.add(MapItem("tx_count", json_name="nTx", _type=int, none_exception=True))
_json_mapper.add(MapItem("size", _type=int, none_exception=True))
_json_mapper.add(MapItem("stripped_size", json_name="strippedsize", _type=int, none_exception=True))
_json_mapper.add(MapItem("weight", _type=int, none_exception=True))


class BtcBlockDAO(BaseDAO):
    __tablename__ = 'Block'

    height = Column(Integer, primary_key=True)
    hash = Column(LargeBinary(16), nullable=False)
    version = Column(Integer, nullable=False)

    block_time = Column(DateTime, nullable=False)
    median_time = Column(DateTime, nullable=False)

    nonce = Column(String, nullable=False)
    bits = Column(LargeBinary(4), nullable=False)

    difficulty = Column(BIGINT, nullable=False)

    tx_count = Column(SMALLINT, nullable=False)
    count_inputs = Column(SMALLINT, nullable=False)
    count_outputs = Column(SMALLINT, nullable=False)

    total_mined = Column(REAL, nullable=False)
    total_reward = Column(REAL, nullable=False)
    total_fee = Column(REAL, nullable=False)
    total_output = Column(REAL, nullable=False)

    size = Column(Integer, nullable=False)
    stripped_size = Column(Integer, nullable=False)
    weight = Column(Integer, nullable=False)

    transactions = relationship("BtcTransactionDAO", back_populates="block")

    @classmethod
    def FromJson(cls, data):
        if len(data.__dict__) not in [18, 19]:
            raise Exception(f"Input data must have exactly 19 keys, but has {len(data.__dict__)}")
        _obj = _json_mapper.from_json(cls, data)

        _obj.transactions = [BtcTransactionDAO.FromJson(a, _obj, index) for index, a in enumerate(data.tx)]
        _obj.count_tx = len(_obj.transactions)

        _obj.total_reward = sum([a.total_output + a.total_fee for a in _obj.transactions if a.is_coinbase])
        _obj.total_fee = sum([a.total_fee for a in _obj.transactions])
        _obj.total_mined = _obj.total_reward - _obj.total_fee

        _obj.count_inputs = sum([a.count_inputs for a in _obj.transactions])
        _obj.count_outputs = sum([a.count_outputs for a in _obj.transactions])
        _obj.total_output = sum([a.total_output for a in _obj.transactions])

        return _obj

    def __init__(self):
        super().__init__()
        self.transactions = []
        self.count_tx = None
        self.total_fee = None
        self.total_output = None
        self.count_inputs = None
        self.count_output = None
        self.total_reward = None
