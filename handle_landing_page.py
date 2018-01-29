#mport DataTable as dt
# -*- coding:utf-8 -*-
import CsvTables as ct
import sys
from collections import OrderedDict, defaultdict
if __name__ == "__main__":
    #列名
    columnsNames = ["us", "search_id", "sum_shows", "sum_clicks", "sum_charge", "sum_zhihang_clicks", "sum_phone_clicks",
                    "sum_appoint_confirm_clicks", "sum_consult_clicks", "sum_collect_clicks", "sum_buy_clicks"]

    if len(sys.argv) < 2:
        raise Exception("too few arguments")


    landingpageTabel = ct.DataTable(sys.argv[1], sep="\t", columnsNames=columnsNames)
    #landingpageTabel = ct.DataTable("20180122_landing.txt", sep="\t", columnsNames=columnsNames)
    #print(landingpageTabel.colNameIdxMap)
    #print(landingpageTabel.storage)

    cols = ["sum_shows", "sum_clicks", "sum_charge", "sum_zhihang_clicks", "sum_phone_clicks", "sum_appoint_confirm_clicks", "sum_consult_clicks", "sum_collect_clicks", "sum_buy_clicks", "search_id"]

    for col in cols:
        if col != "search_id":
            landingpageTabel.asType(col, int)  #  将对应的列转化为int类型
    #按照渠道分组
    tableGroupByUs = landingpageTabel.groupBy(columns="us")
    resultByUs = defaultdict(lambda :defaultdict(lambda :0))
    for us in tableGroupByUs:
        for col in cols:
            if col != "search_id":
                resultByUs[us][col] = sum(tableGroupByUs[us].getColumns(col))
            else: # 使用search_id计算epv
                resultByUs[us]["epv"] = len(set(tableGroupByUs[us].getColumns(col)))
    cols.pop()
    cols = ["us", "epv", ] + cols

    #cols.append("us") # 添加渠道列
    sumByUsTable = ct.DataTable(columnsNames=cols) # 按照渠道计算的总量
    for us in resultByUs:
        row = resultByUs[us]
        row["us"] = us if us is not None else "NULL"
        sumByUsTable.appendRow(row)
    shangjiItem = ["sum_zhihang_clicks", "sum_phone_clicks", "sum_appoint_confirm_clicks", "sum_consult_clicks", "sum_collect_clicks", "sum_buy_clicks"]

    #计算要添加的列
    chargeYuanCol = [float(item) / 100 for item in sumByUsTable.getColumns("sum_charge")]
    sumByUsTable.appendColumns({"sum_charge(元)": chargeYuanCol})
    ctr2Col = sumByUsTable.mapByRow(lambda row: float(row["sum_clicks"]) / row["sum_shows"])
    conversionCol = sumByUsTable.mapByRow(lambda row: sum([row[shangji] for shangji in shangjiItem]))
    sumClicks = sumByUsTable.getColumns("sum_clicks")
    cvrCol = [float(con) / sumClicks[rowIdx] for rowIdx, con in enumerate(conversionCol)]
    asnCol = sumByUsTable.mapByRow(lambda row: float(row["sum_shows"]) / row["epv"])
    cpm3Col = sumByUsTable.mapByRow(lambda row: float(row["sum_charge(元)"]) / row["epv"])

    appendRows = OrderedDict()
    appendRows["conversion"] = conversionCol
    appendRows["ctr2"] = ctr2Col
    appendRows["cvr"] = cvrCol
    appendRows["asn"] = asnCol
    appendRows["cpm3"] = cpm3Col
    sumByUsTable.appendColumns(appendRows)


    print(sumByUsTable)
    newColumns = ["us", "epv", "sum_shows", "sum_clicks", "sum_charge(元)", "sum_zhihang_clicks", "sum_phone_clicks", "sum_appoint_confirm_clicks", "sum_consult_clicks", "sum_collect_clicks", "sum_buy_clicks", "conversion", "ctr2", "cvr", "asn", "cpm3"]  #ordict = dict()
    sumByUsTable.getColumns(newColumns).toCsv(sys.argv[1][:-4] + "output.csv")

    #ordict["A"] = 1
    #ordict["B"] = 2
    #ordict["C"] = 3
    #print(ordict.keys())
