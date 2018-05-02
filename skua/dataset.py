import collections

class DataSet:
    def __init__(self, adapter=None):
        pass

    def add(self, obj):
        if not hasattr(obj, "__hash__"):
            raise TypeError(f"Object {type(obj)} not attribute __hash__")

    def remove(self, rec):
        pass