from sqlalchemy import Column, Integer, SMALLINT, REAL, BOOLEAN, ForeignKey, LargeBinary
from sqlalchemy.orm import relationship

from source_code.DAO.BaseDAO import BaseDAO
from source_code.DAO.BtcInputDAO import BtcInputDAO
from source_code.DAO.BtcOutputDAO import BtcOutputDAO
from source_code.Mapper.MapItem import MapItem
from source_code.Mapper.Mapper import Mapper
from source_code.helpers.IntToBinary4 import IntToBinary4

_json_mapper = Mapper(table_name=None)
_json_mapper.add(MapItem("hash", _type="hex_to_bytes", none_exception=True))
_json_mapper.add(MapItem("txid", _type="hex_to_bytes", none_exception=True))

_json_mapper.add(MapItem("version", _type=int, none_exception=True))
_json_mapper.add(MapItem("total_fee", json_name="fee", _type=float, none_exception=False, none_value=0))
_json_mapper.add(MapItem("lock_time", json_name="locktime", _type=int, none_exception=True))
_json_mapper.add(MapItem("size", _type=int, none_exception=True))
_json_mapper.add(MapItem("vsize", _type=int, none_exception=True))
_json_mapper.add(MapItem("weight", _type=int, none_exception=True))


class BtcTransactionDAO(BaseDAO):
    __tablename__ = 'Transaction'

    id = Column(Integer, primary_key=True)

    block_ref = Column(Integer, ForeignKey('Block.height'), nullable=False, index=True)
    block = relationship("BtcBlockDAO", back_populates="transactions")

    index = Column(SMALLINT, nullable=False)

    hash = Column(LargeBinary(16), nullable=False)
    txid = Column(LargeBinary(16), nullable=False)

    count_inputs = Column(SMALLINT, nullable=False)
    count_outputs = Column(SMALLINT, nullable=False)
    total_output = Column(REAL, nullable=False)
    total_fee = Column(REAL, nullable=False)

    version = Column(IntToBinary4, nullable=False)
    lock_time = Column(IntToBinary4, nullable=False)

    is_coinbase = Column(BOOLEAN, nullable=False)

    inputs = relationship("BtcInputDAO")
    outputs = relationship("BtcOutputDAO")

    @classmethod
    def FromJson(cls, data, block, index):
        if len(data.__dict__) not in [10, 11]:
            raise Exception(f"Input data must have exactly 10 keys but has [{len(data.__dict__)}]")

        _obj = _json_mapper.from_json(cls, data)

        _obj.block = block
        _obj.index = index

        _obj.inputs = [BtcInputDAO.FromJson(i, a, _obj) for i, a in enumerate(data.vin)]
        _obj.count_inputs = len(_obj.inputs)

        _inputs_coinbase = [a for a in _obj.inputs if a.is_coinbase]
        _obj.is_coinbase = len(_inputs_coinbase) > 0

        _obj.outputs = [BtcOutputDAO.FromJson(a, _obj) for a in data.vout]
        _obj.count_outputs = len(_obj.outputs)
        _obj.total_output = sum([a.value for a in _obj.outputs])

        return _obj

    def __init__(self):
        super().__init__()
        self.id = None
        self.block = None
        self.count_inputs = None
        self.count_outputs = None

        self.total_output = -1

        self.inputs = []
        self.outputs = []
