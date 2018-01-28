from collections import defaultdict
from copy import deepcopy
class DataTable:
    """
    一个二维的数据表 类似于CSV文件 或者pandas的DataFrame 
    由行 row 和列 columns 构成
    行索引按照从文件读入的索引确定
    列索引必须使用列名称    
    """
    def __init__(self, csvFileName=None, columnsNames=None, sep=",", encoding="utf-8", dataMat=None):
        """
        从csv文件中读取 
        :param csvFileName 通过CSV文件构造一个新表 目标csv文件
        :param columnsNames csv 文件的列名称 一个可迭代对象 顺序从左至右 默认 为文件第一行的
        :param sep 分割csv文件的字符 默认为','
        :param dataMat 通过字典方式创建 CsvTable 对象 dataMat 格式为 {colName0: [colData0,], colName1: [colData1, ] ...}
        :return: None
        """
        self.colNameIdxMap = {}  #建立名称与列索引的对应关系    
        self.rowLength = 0
        self.columnLength = 0
        if csvFileName is not None: # 从文件创见表       
            with open(csvFileName, "r", encoding=encoding) as f:
                if columnsNames is None:
                    columnList = f.readline();
                else: 
                    columnList = columnsNames
                for idx, item in enumerate(columnList):
                    self.colNameIdxMap[item] = idx #建立名称与列索引的对应关系             
                self.storage = self.private_csvRead(f, seq)
        else: # 从其他函数产生的数据中创建表
            for idx, colName in enumerate(dataMat.keys()):
                self.colNameIdxMap[colName] = idx # 创建列名称 索引位置映射
            rowLength = len(dataMat[dataMat.keys[0]])
            self.storage.append[[None,] * len(dataMat) for i in range(rowLength)] # 构建一个 rowLength * colLength 的 None 矩阵
            for col in dataMat:
                for rowIdx in range(rowLength):
                    self.storage[rowIdx][self.colNameIdxMap[col]] = dataMat[col][rowIdx]
        self.rowLength = len(self.storage)
        self.colLength = len(self.cloumns)
      
    def private_csvRead(self, fileref, sep=","):
        """        
        :param fileName: csv 文件名 要求csv文件完全合法
        :param sep: csv文件分割符 默认为','
        :return: data matricx
        """
        tmpResult = []
        for line in fileref:
            elements = line.strip().split(sep)
            elements = [None if len(e) == 0 else e for e in elements] # 将缺失值设为None
            tmpResult.append(dataMat)
        return tmpResult
        
    def getColumns(self, colNames):
        """
        
        """
        if colNames isinstance list:
            dataMat = {}
            for colName in colNames:
                dataMat[colName] = self.private_get1row(colNames)            
            return DataTable(dataMat)    
        else:
            return private_get1row(colNames)

    def private_get1row(self, colName):
        rowLength = len(self.storage)
        targetColIdx = self.colNameIdxMap[colName]
        return [row[targetColIdx] for row in self.storage]
    
    def toCsv(self, outputFileName, seq=',', encoding="utf-8"):
        keys = self.colNameIdxMap.keys()
        with open(outputFileName, "w", encoding=encoding) as f:
            f.write(seq.join([k for k in keys]) + "\n")
            for row in self.storage:
                f.write(seq.join([row[self.colNameIdxMap[k]] for k in keys]) + "\n")

    def filterOutRows(self, conditionFun):
        """
        :param conditionFun 参数为一个row 该函数对每个row进行映射, 结果为真则结果中保留该row 
                row可以看做一个字典对象 key 为row的column Name 值为该column对应的值
        :return:返回一个新的 DataTable对象
        """
        tmpdict = {}
        for col in self.colNameIdxMap:
            tmpdict[col] = []
        for row in self.storage: 
            tmpRow = {}             
            for k in self.colNameIdxMap.keys()]
                tmpRow[k] = row[self.colNameIdxMap[k]]
                if conditionFun(tmpRow):
                    for k2 in tmpRow:
                        tmpdict[k2].append(tmpRow[k2])
        return DataTable(tmpdict)
    
    def appendRow(self, rowDict):
        """
        在数据表尾部增加一行
        :param rowDict 新行
        """
        tmpRow = []
        if len(set(rowDict.keys()) & set(self.colNameIdxMap.keys())) < len(self.colNameIdxMap.keys()): #检查列名称是否匹配
            raise KeyError("ERROR: Column names mismatch") #新行名称不匹配
        for k in rowDict:
            newRow = [None, ] * self.columnLength;
            newRow[self.colNameIdxMap[k]] = rowDict[k]
        self.rowLength += 1            

    def appendColumn(self, columns):
        """
        :param cloumns 类似于字典的对象 key 为 columnname value为对应的列值 {k1: [v11, v12,..], k2: [v12, v22,..]...}
        :return: 
        """
        for k in columns:
            if len(columns[k]) != self.rowLength:
                raise TypeError("row length mismatch") # 当前要添加的列与原有列长度不匹配
        for colName in columns:
            self.colNameIdxMap[colName] = self.columnLength # 将新的列名称映射到 对应的索引上
            self.columnLength += 1 #列长度更新
            for rowIdx, row in enumerate(self.storage):
                row.append(columns[colName][rowIdx])            
    
    def mapByRow(self, mapFunc, mapMethod="value", newColumnsNames=None):
        """
        :param mapFunc 用来映射每一行的函数
        :param mapMethod 映射结果的方式 
                'value' 将一行映射为一个值 最后返回的结果是一个列表 需要映射函数返回一个单一值
                'row' 将一行映射为新一行 需要保证 映射函数返回一个字典类型的对象
        :param newColumnsNames 在映射为新行的时候指定新行的名称
        :return 'value' 方式返回一个行
        """
        if mapMethod == 'value':
            tmpResult = []
            for row in self.storage:
                tmpRowDict = dict([(k, self.colNameIdxMap[k]) for k in self.colNameIdxMap])
                tmpResult.append(mapFunc(tmpRowDict))
            return tmpResult
        elif mapMethod == 'row':
            dataMat = {}
            for colName in newColumnsNames: #创建表头
                dataMat[colName] = []
            for row in self.storage:
                tmpRowDict = dict([(k, self.colNameIdxMap[k]) for k in self.colNameIdxMap])
                tmpResultRow = mapFunc(tmpRowDict)
                for k in tmpResultRow:
                    if k not in dataMat or len(tmpResultRow) != len(dataMat):
                        raise KeyError("column is not in current column names") # 产生的行与目的行不匹配
                    dataMat[k].append(tmpResultRow[k])       
            return DataTable(dataMat)
        
    def get(self, rowIdx, columnName):
        """
        获取表中的某一个元素
       
        """
        return storage[rowIdx][self.colNameIdxMap[columnName]]

    def set(self, rowIdx, columnName, value):
        """
        设置某一个元素
        """
        self.storage[rowIdx][self.colNameIdxMap[columnName]] = value

    def getShape(self):
        """
        获取表的尺寸 
        return 行数 , 列数
        """
        return self.rowLength, self.columnLength
    
    def asType(self, colName, typeCastFunc):
        """
        将指定的列转化为对应的数据类型
        """
        colValues = self.getColumns(colName)
        valuesCasted = [typeCastFunc(v) for v in colValues] # 防止类型转换出错影响原有数据
        #如果转换有误此处会抛出异常
        for rowIdx, row in enumerate(self.storage):
            row[self.colNameIdxMap[colName]] = valuesCasted[rowIdx]
    
    def groupBy(self, columns, keyFunc=None):
        """
        
        """
        tmpDataMat = dict([(k ,[]) for k in self.colNameIdxMap]) #产生一个只有列名称但是没有数据的表
        groupedDataTable = defaultdict(lambda :DataTable(dataMat=tmpDataMat)) # 根据columns产生的group 
        if cloumns isinstance list:
            #groupKeySetList = [set(self.getColumns(col)) for col in columns]
            for row in self: # 对自身进行迭代
                tmpKey = tuple([row[col] for col in columns])
                groupKey = keyFunc(tmpKey) if keyFunc is not None else tmpKey
                groupedDataTable[groupKey].appendRow(row)
        else:
            keySet = set(getColumns(columns))
            # 为每个 group的key 创建一个对应的空表
            for row in self: # 对自身进行迭代
                #groupKey 是确定将哪一行分到哪一个分组中 groupKey = keyFunc(row[columns])
                groupKey = keyFunc(row[columns]) if keyFunc is not None else row[columns] 
                groupedDataTable[groupKey].appendRow(row)
            return groupedDataTable
    
    def __iter__(self):
        """
        按行迭代DataTable
        """
        self.iterCount = 0
        return self
        
    def __next__(self):
        """
        每次迭代 返回一个字典 key为对应的列名称 value为对应的值
        """
        if self.iterCount < self.rowLength:
            tmpDict = {}
            for k in self.colNameIdxMap:
                tmpDict[k] = self.storage[rowIdx][self.colNameIdxMap[k]]
            self.iterCount += 1
            return tmpDict
        else:
            raise StopIteration
          
    def __str__():
        """
        """
        keys = self.colNameIdxMap.keys()
        strlist = []
        strlist.append("  |  ".join([str(k) for k in keys]))
        for row in self:
            strlist.append("  |  ".join([str(row[k]) for k in keys]))
        return "\n".join(strlist) 
        
    """
    def private_Cartesian_product(self, setList):
        """
        产生笛卡尔积
        """
        tmpResult = []
        for i in range(len(setList)): #添加一个新的set
            newResult = [] #装入本轮结果的变量
            for setElement in setList[i]: #添加一个set中的一个新元素
                for t_row in tmpResult.deepcopy(): #为上次结果的每一行添加一个新的元素
                    t_row.append(setElement)
                    newResult.append(t_row)
            tmpResult = newResult
        return [tuple(row) for row in tmpResult]   
    """
    



