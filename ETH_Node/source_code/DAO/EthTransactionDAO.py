from sqlalchemy import PrimaryKeyConstraint, Column, Integer, ForeignKey, SMALLINT, REAL, Boolean, LargeBinary, BigInteger
from sqlalchemy.dialects.postgresql import BYTEA
from sqlalchemy.orm import relationship

from source_code.DAO.BaseDAO import BaseDAO
from source_code.Mapper.MapItem import MapItem
from source_code.Mapper.Mapper import Mapper, check_fields

_json_mapper = Mapper()
_json_mapper.add(MapItem("blockNumber", _type="hex_to_int", none_exception=True))

_json_mapper.add(MapItem("hash", _type="str_to_bytes", none_exception=True))
_json_mapper.add(MapItem("index", json_name="transactionIndex", _type="hex_to_int", none_exception=True))
_json_mapper.add(MapItem("transaction_type", json_name="type", _type="hex_to_int", none_exception=True))

_json_mapper.add(MapItem("from_adr", json_name="from", _type="str_to_bytes", none_exception=True))
_json_mapper.add(MapItem("to_adr", json_name="to", _type="str_to_bytes", none_exception=False))
_json_mapper.add(MapItem("contract_address", json_name="receipt.contractAddress", _type="str_to_bytes", none_exception=False))

_json_mapper.add(MapItem("value", _type="hex_to_int", none_exception=True))

_json_mapper.add(MapItem("gas_limit", json_name="gas", _type="hex_to_int", none_exception=True))
_json_mapper.add(MapItem("gas_used", json_name="receipt.gasUsed", _type="hex_to_int", none_exception=True))
_json_mapper.add(MapItem("gas_price", json_name="gasPrice", _type="hex_to_int", none_exception=True))
_json_mapper.add(MapItem("gas_fee_max", json_name="maxFeePerGas", _type="hex_to_int", none_exception=False, none_value=0))
_json_mapper.add(MapItem("gas_fee_max_priority", json_name="maxPriorityFeePerGas", _type="hex_to_int", none_exception=False, none_value=0))

_json_mapper.add(MapItem("gas_blob_used", json_name="receipt.blobGasUsed", _type="hex_to_int", none_exception=False, none_value=0))
_json_mapper.add(MapItem("gas_blob_price", json_name="receipt.blobGasPrice", _type="hex_to_int", none_exception=False, none_value=0))

_json_mapper.add(MapItem("input_data", json_name="input", _type="str_to_bytes", none_exception=True))
_json_mapper.add(MapItem("nonce", _type="str_to_bytes", none_exception=True))
_json_mapper.add(MapItem("status", _type="hex_to_int", none_exception=False))


class EthTransactionDAO(BaseDAO):
    __tablename__ = 'Transaction'
    __table_args__ = (
        PrimaryKeyConstraint('block_ref', 'index'),
    )

    block_ref = Column(Integer, ForeignKey('Block.id'), nullable=False, index=True)
    block = relationship("EthBlockDAO", back_populates="transactions")

    index = Column(SMALLINT, nullable=False)
    transaction_type = Column(SMALLINT, nullable=False)

    value = Column(REAL, nullable=False)
    tx_fee = Column(REAL, nullable=False)

    status = Column(SMALLINT, nullable=True)

    count_logs = Column(SMALLINT, nullable=False)
    count_blob_versioned_hashes = Column(SMALLINT, nullable=False)
    count_access_list = Column(SMALLINT, nullable=False)

    gas_limit = Column(Integer, nullable=False)
    gas_used = Column(Integer, nullable=False)

    gas_price = Column(REAL, nullable=False)
    gas_fee_max = Column(REAL, nullable=True)
    gas_fee_max_priority = Column(REAL, nullable=True)
    gas_blob_used = Column(Integer, nullable=False)
    gas_blob_price = Column(BigInteger, nullable=False)

    has_input_data = Column(Boolean, nullable=False)
    has_contract_adr = Column(Boolean, nullable=False)

    hash = Column(LargeBinary(32), nullable=False)
    from_adr = Column(LargeBinary(20), nullable=False)
    to_adr = Column(LargeBinary(20), nullable=True)

    contract_adr = Column(LargeBinary(20), nullable=True)
    nonce = Column(LargeBinary(8), nullable=False)
    input_data = Column(BYTEA, nullable=True)

    @classmethod
    def FromJson(cls, data, block):
        check_fields(data, length=[11, 12],
                     fields=["blockNumber", "from", "gas", "hash", "input", "to", "type", "value", "transactionIndex"],
                     ignore=["receipt", "yParity", "chainId", 'maxFeePerGas', 'maxPriorityFeePerGas', 'blobVersionedHashes', 'accessList'])

        check_fields(data.receipt, length=[8, 9],
                     fields=["gasUsed", "from", "logs", 'blockNumber'],
                     ignore=["blobGasUsed", "blobGasPrice"])

        _obj = _json_mapper.from_json(cls, data)
        _obj.block = block

        _obj.logs = data.receipt.logs
        _obj.count_logs = len(_obj.logs)

        if _obj.transaction_type not in [0, 1, 2, 3]:
            raise NotImplemented("Flow")

        if _obj.status is not None and _obj.status not in [0]:
            raise NotImplemented("Flow")

        _obj.value = _obj.value / int(1e18)
        _obj.gas_price = _obj.gas_price / int(1e9)
        _obj.tx_fee = _obj.gas_used * _obj.gas_price / int(1e9)

        _obj.gas_fee_max = float(_obj.gas_fee_max) / int(1e9)
        _obj.gas_fee_max_priority = float(_obj.gas_fee_max_priority) / int(1e9)

        _obj.has_input_data = _obj.input_data is not None

        _obj.count_blob_versioned_hashes = len(data.get("blobVersionedHashes", []))
        _obj.count_access_list = len(data.get("accessList", []))

        _obj.has_contract_adr = _obj.contract_address is not None

        return _obj

    def __init__(self):
        super().__init__()
