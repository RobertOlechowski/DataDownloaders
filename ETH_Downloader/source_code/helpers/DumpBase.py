class DumpBase(object):

    def dump(self, indent_level=0):
        sp = "\t" * indent_level
        for k, value in self.__dict__.items():
            if isinstance(value, DumpBase):
                print(f"{sp}{k}:")
                value.dump(indent_level + 1)
                continue

            print(f"{sp}{k:<16}: {value}")
