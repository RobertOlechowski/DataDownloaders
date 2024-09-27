from sqlalchemy.types import TypeDecorator, LargeBinary


class IntToBinary4(TypeDecorator):
    impl = LargeBinary

    def load_dialect_impl(self, dialect):
        return dialect.type_descriptor(LargeBinary(4))

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        return value.to_bytes(4, byteorder='big', signed=False)

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return int.from_bytes(value, byteorder='big', signed=False)
