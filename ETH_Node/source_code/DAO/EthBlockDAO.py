from sqlalchemy import Column, Integer, DateTime, SMALLINT, REAL, LargeBinary
from sqlalchemy.orm import relationship

from source_code.DAO.BaseDAO import BaseDAO
from source_code.DAO.EthTransactionDAO import EthTransactionDAO
from source_code.DAO.EthUncleDAO import EthUncleDAO
from source_code.DAO.WithdrawalsDAO import WithdrawalsDAO
from source_code.Mapper.MapItem import MapItem
from source_code.Mapper.Mapper import Mapper, check_fields

_json_mapper = Mapper()
_json_mapper.add(MapItem("number", _type="hex_to_int", none_exception=True))

_json_mapper.add(MapItem("timestamp", _type="hex_to_int_time_utc", none_exception=True))

_json_mapper.add(MapItem("difficulty", _type="hex_to_int", none_exception=True))
_json_mapper.add(MapItem("total_difficulty", json_name="totalDifficulty", _type="hex_to_int", none_exception=True))

_json_mapper.add(MapItem("gas_limit", json_name="gasLimit", _type="hex_to_int", none_exception=True))
_json_mapper.add(MapItem("gas_used", json_name="gasUsed", _type="hex_to_int", none_exception=True))
_json_mapper.add(MapItem("gas_base_fee", json_name="baseFeePerGas", _type="hex_to_int", none_exception=False))
_json_mapper.add(MapItem("gas_excess_blob", json_name="excessBlobGas", _type="hex_to_int", none_exception=False))
_json_mapper.add(MapItem("gas_blob_used", json_name="blobGasUsed", _type="hex_to_int", none_exception=False))

_json_mapper.add(MapItem("size", _type="hex_to_int", none_exception=True))

_json_mapper.add(MapItem("hash", _type="str_to_bytes", none_exception=True))
_json_mapper.add(MapItem("extra_data", json_name="extraData", _type="str_to_bytes", none_exception=True))
_json_mapper.add(MapItem("miner", _type="str_to_bytes", none_exception=True))
_json_mapper.add(MapItem("nonce", _type="str_to_bytes", none_exception=True))

_json_mapper.add(MapItem("withdrawals_root", json_name="withdrawalsRoot", _type="str_to_bytes", none_exception=False))


def get_block_reward(block_number):
    if block_number < 4370000:
        return 5
    if block_number < 7280000:
        return 3
    if block_number < 15537393:
        return 2
    return 0


class EthBlockDAO(BaseDAO):
    __tablename__ = 'Block'

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, nullable=False)

    count_tx = Column(SMALLINT, nullable=False)
    count_uncles = Column(SMALLINT, nullable=False)
    count_withdrawals = Column(SMALLINT, nullable=False)

    block_reward = Column(SMALLINT, nullable=False)

    nephew_reward = Column(REAL, nullable=False)
    tx_fee_reward = Column(REAL, nullable=False)
    gas_burnt_fee = Column(REAL, nullable=True)
    total_reward = Column(REAL, nullable=False)
    uncles_reward = Column(REAL, nullable=False)

    gas_base_fee = Column(REAL, nullable=True)

    gas_limit = Column(Integer, nullable=False)
    gas_used = Column(Integer, nullable=False)
    gas_excess_blob = Column(Integer, nullable=True)
    gas_blob_used = Column(Integer, nullable=True)

    miner = Column(LargeBinary(20), nullable=False)
    nonce = Column(LargeBinary(8), nullable=False)
    extra_data = Column(LargeBinary(32), nullable=True)

    transactions = relationship("EthTransactionDAO", back_populates="block")
    uncles = relationship("EthUncleDAO", back_populates="block")
    withdrawals = relationship("WithdrawalsDAO", back_populates="block")

    @classmethod
    def FromJson(cls, data):
        check_fields(data, length=[14],
                     fields=["number", "hash", "nonce", "size", "timestamp", "transactions"],
                     ignore=['baseFeePerGas', "withdrawalsRoot", "withdrawals", "excessBlobGas", "blobGasUsed", "parentBeaconBlockRoot"])

        _obj = _json_mapper.from_json(cls, data)

        _obj.id = _obj.number
        _obj.block_reward = get_block_reward(_obj.number)

        if _obj.difficulty == 0:
            _obj.difficulty = None

        _obj.transactions = [EthTransactionDAO.FromJson(a, _obj) for a in data.transactions]
        _obj.transactions_input_data = [a.input_data for a in _obj.transactions if a.has_input_data]
        _obj.uncles = [EthUncleDAO.FromJson(a, _obj, index) for index, a in enumerate(data.uncles)]

        _obj.count_tx = len(_obj.transactions)
        _obj.count_uncles = len(_obj.uncles)

        _obj.block_reward = get_block_reward(_obj.number)
        _obj.nephew_reward = float(_obj.count_uncles) * float(_obj.block_reward) / 32.0
        _obj.tx_fee_reward = sum([a.tx_fee for a in _obj.transactions]) or 0
        _obj.uncles_reward = sum([a.uncle_reward for a in _obj.uncles]) or 0
        if _obj.gas_base_fee is not None:
            _obj.gas_base_fee = _obj.gas_base_fee / int(1e9)
            _obj.gas_burnt_fee = _obj.gas_used * _obj.gas_base_fee / int(1e9)

        _temp_gas_burnt_fee = _obj.gas_burnt_fee or 0.0
        _obj.total_reward = float(_obj.block_reward) + _obj.nephew_reward + float(_obj.tx_fee_reward) - _temp_gas_burnt_fee

        _obj.withdrawals = [WithdrawalsDAO.FromJson(a, _obj) for a in data.get("withdrawals", [])]
        _obj.count_withdrawals = len(_obj.withdrawals)

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
