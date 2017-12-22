class CsvTable:
    """
    一个二维的数据表 类似于CSV文件  
    由行 row 和列 columns 构成 
        
    """
    def __init__(self, csvFileName=None):
        """
        从csv文件中读取 
        :param csvFileName 通过CSV文件构造一个新表
        :return: None
        """
        self.storage = []
        self.columnsIndex = []
        #self.rowIndex = []
        private_csvRead()
    def private_csvRead(self, fileName, sep=","):
        """        
        :param fileName: csv 文件名 要求csv文件完全合法
        :param sep: csv文件分割符 默认为','
        :return: None
        """
        columnIndex = {}
        f = open(fileName, "r", encoding="utf-8")
        firstLine = f.readline().strip()
        tmpColnames = firstLine.split(sep)
        for i in range(len(tmpColnames)):
            columnIndex[tmpColnames[i]] = i
        for line in f:
            datas = line.strip().split(sep)
            self.storage.append(datas)

    def readCsv(self, csvfileName):
        """        
        同带有文件名称参数的 csvFileName
        :param csvfileName: 
        :return: 
        """
        pass

    def toCsv(self, outputFileName):
        pass


    def filterOutRows(self, condition):
        pass

    def apply(self, func, attrNames=None):
        pass

    def get(self, row, column):
        pass

    def set(self, row, column):
        pass

    def getRows(self):
        pass

    def getColumns(self):
        pass

    def getShape(self):
        pass

    def appendRows(self):
        pass

    def appendColumns(self):
        pass




