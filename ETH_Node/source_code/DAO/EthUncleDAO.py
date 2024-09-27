from sqlalchemy import Column, Integer, SMALLINT, DateTime, REAL, LargeBinary, PrimaryKeyConstraint, ForeignKey
from sqlalchemy.orm import relationship

from source_code.DAO.BaseDAO import BaseDAO
from source_code.Mapper.MapItem import MapItem
from source_code.Mapper.Mapper import Mapper

_json_mapper = Mapper()
_json_mapper.add(MapItem("number", _type="hex_to_int", none_exception=True))
_json_mapper.add(MapItem("timestamp", _type="hex_to_int_time_utc", none_exception=True))
_json_mapper.add(MapItem("hash", _type="str_to_bytes", none_exception=True))
_json_mapper.add(MapItem("extra_data", json_name="extraData", _type="str_to_bytes", none_exception=True))
_json_mapper.add(MapItem("miner", _type="str_to_bytes", none_exception=True))
_json_mapper.add(MapItem("nonce", _type="str_to_bytes", none_exception=True))


class EthUncleDAO(BaseDAO):
    __tablename__ = 'Uncle'
    __table_args__ = (
        PrimaryKeyConstraint('block_ref', 'index'),
    )

    block_ref = Column(Integer, ForeignKey('Block.id'), nullable=False, index=True)
    block = relationship("EthBlockDAO", back_populates="uncles")

    index = Column(SMALLINT, nullable=False)

    number = Column(Integer, nullable=False)
    delta = Column(Integer, nullable=False)
    timestamp = Column(DateTime, nullable=False)
    uncle_reward = Column(REAL, nullable=False)

    miner = Column(LargeBinary(20), nullable=False)
    nonce = Column(LargeBinary(8), nullable=False)
    extra_data = Column(LargeBinary(32), nullable=True)

    @classmethod
    def FromJson(cls, data, block, index):
        if len(data.uncles) != 0:
            raise Exception()

        _obj = _json_mapper.from_json(cls, data)
        _obj.block = block
        _obj.index = index

        _obj.delta = block.number - _obj.number
        multiplier = (8.0 + _obj.number - block.number) / 8.0
        _obj.uncle_reward = multiplier * block.block_reward

        return _obj

    def __init__(self):
        super().__init__()
        self.block = None
        self.uncle_index = None
        self.uncle_reward = None
        self.delta = None
