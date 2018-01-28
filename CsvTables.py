# -*- coding:utf-8 -*-
from collections import defaultdict, OrderedDict
from copy import deepcopy
class DataTable:
    """
    描述:
    一个二维的数据表 类似于CSV文件 或者pandas的DataFrame 
    由行 row 和列 columns 构成
    行索引按照从文件读入的索引确定
    列索引必须使用列名称 
       
    基本操作:
    1.可以读写任意一个单元格中的内容 注意保持类型与原有类型一致
    2.可以在表的尾部增加行 或者增加一列 但是需要保证对应维度的匹配 
    3.可以按照行迭代 每行可以看作是一个字典 字典的key是列名称 value是该行该列下的值
    """
    def __init__(self, csvFileName=None, columnsNames=None, sep=",", dataMat=None):
        """
        从csv文件中读取 
        :param csvFileName 若不为None表示 通过CSV文件构造一个新表 目标csv文件 
        :param columnsNames 若不为None 表示在csv文件之外 指定 csv 文件的列名称 一个可迭代对象 顺序从左至右 默认 为文件第一行的
        :param sep 分割csv文件的字符 默认为','
        :param dataMat 若不为None表示 通过字典方式创建 CsvTable 对象 dataMat 格式为 {colName0: [colData0,], colName1: [colData1, ] ...}
        :return: None
        :test status: succeed
        """
        self.colNameIdxMap = OrderedDict()  #建立名称与列索引的对应关系
        self.rowLength = 0
        self.colLength = 0
        self.storage = []
        # $todo 增加列类型记录
        if csvFileName is not None: # 从文件创见表       
            with open(csvFileName, "r") as f:
                if columnsNames is None:
                    columnList = f.readline().strip().split(sep)
                else: 
                    columnList = columnsNames
                for idx, item in enumerate(columnList):
                    self.colNameIdxMap[item] = idx #建立名称与列索引的对应关系             
                self.storage = self.private_csvRead(f, sep)

        else: # 从其他函数产生的数据中创建表
            # todo: 检查输入的合法性 不应该是一个空字典

            for idx, colName in enumerate(dataMat.keys()):
                self.colNameIdxMap[colName] = idx # 创建列名称 索引位置映射
            rowLength = len(dataMat[dataMat.keys()[0]])
            for i in range(rowLength):
                self.storage.append([None,] * len(dataMat))
            # 构建一个 rowLength * colLength 的 None 矩阵
            for col in dataMat:
                for rowIdx in range(rowLength):
                    self.storage[rowIdx][self.colNameIdxMap[col]] = dataMat[col][rowIdx]
        self.rowLength = len(self.storage)
        self.colLength = len(self.colNameIdxMap)

    # 文件读写操作
    def private_csvRead(self, fileref, sep=","):
        """        
        :param fileName: csv 文件名 要求csv文件完全合法
        :param sep: csv文件分割符 默认为','
        :return: data matrix
        :test status: succeed
        """
        tmpResult = []
        for line in fileref:
            elements = line.strip().split(sep)
            elements = [None if len(e) == 0 else e for e in elements] # 将缺失值设为None
            tmpResult.append(elements)
        return tmpResult
    
    def toCsv(self, outputFileName, seq=',', ignoreColName=False):
        """        
        :param outputFileName: 输出文件名 
        :param seq: 分割符
        :param ignoreColName: 是否输出标题
        :return:
        :test status: Succeed
        """
        keys = self.colNameIdxMap.keys()
        with open(outputFileName, "w") as f:
            if not ignoreColName:
                f.write(seq.join([k for k in keys]) + "\n")
            for row in self.storage:
                f.write(seq.join([str(row[self.colNameIdxMap[k]]) for k in keys]) + "\n")

    # 元素读写操作
    def get(self, rowIdx, columnName):
        """
        获取表中的某一个元素
        :test status: Succeed 
        """
        return self.storage[rowIdx][self.colNameIdxMap[columnName]]

    def set(self, rowIdx, columnName, value):
        """
        设置某一个元素
        :test status: Succeed
        """
        # $todo 设置时检查类型
        self.storage[rowIdx][self.colNameIdxMap[columnName]] = value

    def private_get1column(self, colName):
        """        
        :param colName: 
        :return: 获取单一的列
        :test status: Succeed
        """
        targetColIdx = self.colNameIdxMap[colName]
        return [row[targetColIdx] for row in self.storage]

    # 列操作
    def getColumns(self, colNames):
        """
        获取对应名称的列
        :param colNames 对应的列名称 如果为单个字符串 那就取单独这个字符串对应的列 
                如果是一个列表, 那就按照列表顺序将取出的几个列组装成一个新的表
        :return 返回单一的列或者组合成的表
        :test status: Succeed
        """
        if isinstance(colNames, list):
            dataMat = {}
            for colName in colNames:
                dataMat[colName] = self.private_get1column(colName)
            return DataTable(dataMat=dataMat)
        else:
            return self.private_get1column(colNames)

    def appendColumn(self, columns):
        """
        :param cloumns 类似于字典的对象 key 为 columnname value为对应的列值 {k1: [v11, v12,..], k2: [v12, v22,..]...}
        :return: 
        :test status: Succeed
        """
        for k in columns:
            if len(columns[k]) != self.rowLength:
                raise TypeError("row length mismatch")  # 当前要添加的列与原有列长度不匹配
        for colName in columns:
            self.colNameIdxMap[colName] = self.colLength  # 将新的列名称映射到 对应的索引上
            self.colLength += 1  # 列长度更新
            for rowIdx, row in enumerate(self.storage):
                row.append(columns[colName][rowIdx])

    def get_1Row(self, rowIdx):
        """ 
        获取对应行索引的行
        :param rowIdx: 数字行索引 
        :return: 
        :test status
        """
        tmpRowDict = {}
        for k in self.colNameIdxMap:
            tmpRowDict[k] = self.storage[rowIdx][self.colNameIdxMap[k]]
        return tmpRowDict

    # 行操作
    def getRows(self, startIdx, endIdx):
        """        
        按照行索引获取Table的分片
        :param startIdx 包含起始索引
        :param endIdx 不包含结束索引
        :return: 新的DataTable
        :test status
        """
        dataMat = OrderedDict()
        for k in self.colNameIdxMap:
            dataMat[k] = []

        for rowIdx in range(startIdx, endIdx):
            for k in self.colNameIdxMap:
                dataMat[k].append(self.storage[rowIdx][self.colNameIdxMap[k]])
        return DataTable(dataMat=dataMat)

    def filterOutRows(self, conditionFun):
        """
        :param conditionFun 参数为一个row 该函数对每个row进行映射, 结果为真则结果中保留该row 
                row可以看做一个字典对象 key 为row的column Name 值为该column对应的值
        :return:返回一个新的 DataTable对象
        :test status: Succeed
        """
        tmpdict = OrderedDict()
        for col in self.colNameIdxMap:
            tmpdict[col] = []
        filteredTable = DataTable(dataMat=tmpdict)
        for row in self:
            if conditionFun(row):
                filteredTable.appendRow(row) # 结果为true 则添加这一行到结果中
        return filteredTable

    def appendRow(self, rowDict):
        """
        在数据表尾部增加一行
        :param rowDict 新行
        :test status: succeed
        """
        tmpRow = []
        if len(set(rowDict.keys()) & set(self.colNameIdxMap.keys())) < len(self.colNameIdxMap.keys()):  # 检查列名称是否匹配
            raise KeyError("ERROR: Column names mismatch")  # 新行名称不匹配
        newRow = [None, ] * self.colLength
        for k in rowDict:
            newRow[self.colNameIdxMap[k]] = rowDict[k]
        self.storage.append(newRow)
        self.rowLength += 1

    def mapByRow(self, mapFunc, mapMethod="value", newColumnsNames=None):
        """
        :param mapFunc 用来映射每一行的函数
        :param mapMethod 映射结果的方式 
                'value' 将一行映射为一个值 最后返回的结果是一个列表 需要映射函数返回一个单一值
                'row' 将一行映射为新一行 需要保证 映射函数返回一个字典类型的对象
        :param newColumnsNames 在映射为新行的时候指定新行的名称
        :return 'value' 方式返回一个行
        :test status: succeed
        """
        if mapMethod == 'value':
            tmpResult = []
            for row in self:
                tmpResult.append(mapFunc(row))
            return tmpResult
        elif mapMethod == 'row':
            dataMat = OrderedDict()
            for colName in newColumnsNames:  # 创建表头
                dataMat[colName] = []
            resultTable = DataTable(dataMat=dataMat)
            for row in self:
                tmpResultRow = mapFunc(row)
                resultTable.appendRow(tmpResultRow)
            return resultTable

    # 获取信息
    def getShape(self):
        """
        获取表的尺寸 
        return 行数 , 列数
        testStatus: Succeed
        """
        return self.rowLength, self.colLength

    # 设置类型
    def asType(self, colName, typeCastFunc):
        """
        将指定的列转化为对应的数据类型 该操作为原地操作 会改变表的内容
        :param colName 要转换的列名称 
        :param typeCastFunc 目标类型转换函数 或者类的构造函数 该函数的参数为当前列中的值
               比如要将对应的列转换为int 类型 那么这个函数就是int 
        :return 
        :test status: Succeed
        """
        colValues = self.getColumns(colName)
        valuesCasted = [typeCastFunc(v) for v in colValues] # 防止类型转换出错影响原有数据
        # 如果转换有误此处会抛出异常
        for rowIdx, row in enumerate(self.storage):
            row[self.colNameIdxMap[colName]] = valuesCasted[rowIdx]

    #分组操作
    def groupBy(self, columns, keyFunc=None):
        """        
        :param columns: 
        :param keyFunc: 
        :return:
        :test status:  
        """
        tmpDataMat = OrderedDict()
        for col in self.colNameIdxMap:
            tmpDataMat[col] = []
        #tmpDataMat = dict([(k ,[]) for k in self.colNameIdxMap]) # 产生一个只有列名称但是没有数据的表
        groupedDataTable = defaultdict(lambda :DataTable(dataMat=tmpDataMat)) # 根据columns产生的group 
        if isinstance(columns, list):
            # groupKeySetList = [set(self.getColumns(col)) for col in columns]
            for row in self: # 对自身进行迭代
                tmpKey = tuple([row[col] for col in columns])
                groupKey = keyFunc(tmpKey) if keyFunc is not None else tmpKey
                groupedDataTable[groupKey].appendRow(row)
        else:
            # 为每个 group的key 创建一个对应的空表
            for row in self: # 对自身进行迭代
                # groupKey 是确定将哪一行分到哪一个分组中 groupKey = keyFunc(row[columns])
                groupKey = keyFunc(row[columns]) if keyFunc is not None else row[columns] 
                groupedDataTable[groupKey].appendRow(row)
        return groupedDataTable

    # 迭代操作
    def __iter__(self):
        """
        每次迭代 返回一个字典 key为对应的列名称 value为对应的值
        按行迭代DataTable
        :test status:  
        """
        self.iterCount = 0 # 迭代位置设置为0
        return self

    def next(self):
        """
        每次迭代 返回一个字典 key为对应的列名称 value为对应的值
        :test status:  
        """
        if self.iterCount < len(self.storage):
            tmpDict = {}
            for k in self.colNameIdxMap:
                tmpDict[k] = self.storage[self.iterCount][self.colNameIdxMap[k]]
            self.iterCount += 1
            return tmpDict
        else:
            raise StopIteration

    # 显示
    def __str__(self):
        """
        打印使用的字符串
        :test status:  
        """
        keys = self.colNameIdxMap.keys()
        strlist = []
        strlist.append("\t".join([str(k) for k in keys]))
        strlist.append("\t" * len(self.colNameIdxMap))
        for row in self:
            strlist.append("\t".join([str(row[k]) for k in keys]))
        return "\n".join(strlist) 
        

    



