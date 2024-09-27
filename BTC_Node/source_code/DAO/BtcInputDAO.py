from sqlalchemy import Column, Integer, ForeignKey, PrimaryKeyConstraint, SMALLINT, BOOLEAN, LargeBinary
from sqlalchemy.orm import relationship

from source_code.DAO.BaseDAO import BaseDAO
from source_code.Mapper.MapItem import MapItem
from source_code.Mapper.Mapper import Mapper

_json_mapper = Mapper(table_name=None)
_json_mapper.add(MapItem("sequence", _type=int, none_exception=True))
_json_mapper.add(MapItem("target_transaction", json_name="txid", _type="hex_to_bytes", none_exception=False))
_json_mapper.add(MapItem("coinbase", _type="hex_to_bytes", none_exception=False))
_json_mapper.add(MapItem("vout", _type=int, none_exception=False))


class BtcInputDAO(BaseDAO):
    __tablename__ = 'Input'
    __table_args__ = (
        PrimaryKeyConstraint('transaction_ref', 'index'),
    )

    transaction_ref = Column(Integer, ForeignKey('Transaction.id'), nullable=False)
    transaction = relationship("BtcTransactionDAO", back_populates="inputs")

    index = Column(SMALLINT, nullable=False)
    is_coinbase = Column(BOOLEAN, nullable=False)
    target_transaction = Column(LargeBinary(16), nullable=True)
    vout = Column(SMALLINT, nullable=True)

    @classmethod
    def FromJson(cls, index, data, transaction):
        _obj = _json_mapper.from_json(cls, data)
        _obj.transaction = transaction
        _obj.index = index
        _obj.sequence = _obj.sequence.to_bytes(4, byteorder='big')

        if len(data.__dict__) in [4, 5] and all(item in data.__dict__ for item in ["sequence", "txid", "vout"]):
            _obj.is_coinbase = False
            return _obj

        if len(data.__dict__) in [2, 3] and all(item in data.__dict__ for item in ["coinbase", "sequence"]):
            _obj.is_coinbase = True
            return _obj

        raise Exception(f"Input data size = [{len(data.__dict__)}]")

    def __init__(self):
        super().__init__()
        self.transaction = None
