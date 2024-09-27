from sqlalchemy import Column, SMALLINT, REAL, String, Integer, ForeignKey, PrimaryKeyConstraint
from sqlalchemy.orm import relationship

from source_code.DAO.BaseDAO import BaseDAO
from source_code.Mapper.MapItem import MapItem
from source_code.Mapper.Mapper import Mapper
from source_code.helpers.Enums import ScriptTypeEnum

_mapper = Mapper(table_name=None)
_mapper.add(MapItem("script_type_name", json_name="scriptPubKey.type", _type=str, none_exception=True))
_mapper.add(MapItem("script_address", json_name="scriptPubKey.address", _type=str, none_exception=False))
_mapper.add(MapItem("index", json_name="n", _type=int, none_exception=True))
_mapper.add(MapItem("value", _type=float, none_exception=True))


class BtcOutputDAO(BaseDAO):
    __tablename__ = 'Output'
    __table_args__ = (
        PrimaryKeyConstraint('transaction_ref', 'index'),
    )

    transaction_ref = Column(Integer, ForeignKey('Transaction.id'), nullable=False)
    transaction = relationship("BtcTransactionDAO", back_populates="outputs")

    index = Column(SMALLINT, nullable=False)
    value = Column(REAL, nullable=False)

    script_address = Column(String, nullable=True)
    script_type = Column(SMALLINT, nullable=False)

    @classmethod
    def FromJson(cls, data, transaction):
        if len(data.__dict__) == 3 and all(item in data.__dict__ for item in ["scriptPubKey", "value", "n"]):
            _obj = _mapper.from_json(cls, data)
            _obj.transaction = transaction
            _obj.script_type = ScriptTypeEnum[_obj.script_type_name]
            return _obj

        raise Exception("Input data must have exactly 2 keys")

    def __init__(self):
        super().__init__()
        self.transaction = None
        self.has_address = None
        self.script_type = None
