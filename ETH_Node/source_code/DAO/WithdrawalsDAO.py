from sqlalchemy import PrimaryKeyConstraint, Column, Integer, ForeignKey, REAL, LargeBinary
from sqlalchemy.orm import relationship

from source_code.DAO.BaseDAO import BaseDAO
from source_code.Mapper.MapItem import MapItem
from source_code.Mapper.Mapper import Mapper, check_fields

_json_mapper = Mapper()
_json_mapper.add(MapItem("to_adr", json_name="address", _type="str_to_bytes", none_exception=True))
_json_mapper.add(MapItem("amount", json_name="amount", _type="hex_to_int", none_exception=True))
_json_mapper.add(MapItem("withdraw_index", json_name="index", _type="hex_to_int", none_exception=True))
_json_mapper.add(MapItem("validator_index", json_name="validatorIndex", _type="hex_to_int", none_exception=True))


class WithdrawalsDAO(BaseDAO):
    __tablename__ = 'Withdrawal'
    __table_args__ = (
        PrimaryKeyConstraint('block_ref', 'withdraw_index'),
    )

    block_ref = Column(Integer, ForeignKey('Block.id'), nullable=False, index=True)
    block = relationship("EthBlockDAO", back_populates="withdrawals")
    withdraw_index = Column(Integer, nullable=False)

    validator_index = Column(Integer, nullable=False)
    to_adr = Column(LargeBinary(20), nullable=False)
    amount = Column(REAL, nullable=False)

    @classmethod
    def FromJson(cls, data, block):
        check_fields(data, length=[4], fields=["address", "amount", "index", "validatorIndex"], ignore=[])
        _obj = _json_mapper.from_json(cls, data)
        _obj.block = block
        _obj.amount = float(_obj.amount) / int(1e9)
        return _obj

    def __init__(self):
        super().__init__()
        self.block = None
