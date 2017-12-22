class CsvVector:
    """
    向量 
    可执行操作
    标量乘法
    內积
    """
    def __init__(self, iterObject=None):
        if iterObject is None:
            self.storage = []
        else:
            self.storage = [e for e in iterObject]

    def __len__(self):
        return len(self.storage)

    def __add__(self, other):
        if isinstance(other, Vector):
            if len(other) == len(self.storage):
                return Vector(iterObject=[for pair in])
            else:
                raise ValueError("Unsupport type")
        else:
            raise TypeError("Unsupport type")

    def __iter__(self):