#mport DataTable as dt
# -*- coding:utf-8 -*-
import CsvTables as ct
from collections import OrderedDict, defaultdict
if __name__ == "__main__":
    columnsNames = ["us", "search_id", "sum_shows", "sum_clicks", "sum_charge", "sum_zhihang_clicks", "sum_phone_clicks",
                    "sum_appoint_confirm_clicks", "sum_consult_clicks", "sum_collect_clicks", "sum_buy_clicks"]
    landingpageTabel = ct.DataTable("24.txt", sep=chr(1), columnsNames=columnsNames)
    print(landingpageTabel.head(2))

    containEmptySid = landingpageTabel.getColumns("search_id")
    emptyEpv = len(set(containEmptySid))
    nonEmptySid = landingpageTabel.filterOutRows(lambda row: row["us"] is not None).getColumns("search_id")
    nonEmptyEpv = len(set(nonEmptySid))
    print(emptyEpv, nonEmptyEpv)


    cols = ["sum_shows", "sum_clicks", "sum_charge", "sum_zhihang_clicks", "sum_phone_clicks", "sum_appoint_confirm_clicks", "sum_consult_clicks", "sum_collect_clicks", "sum_buy_clicks"]

    for col in cols:
        landingpageTabel.asType(col, int)  #  将对应的列转化为int类型

    tableGroupByUs = landingpageTabel.groupBy(columns="us")
    resultByUs = defaultdict(lambda :defaultdict(lambda :0))
    for us in tableGroupByUs:
        for col in cols:
            resultByUs[us][col] = sum(tableGroupByUs[us][col])

    cols.append("us") # 添加渠道列
    sumByUsTable = ct.DataTable(cols) # 按照渠道计算的总量
    for us in resultByUs:
        row = resultByUs[us]
        row["us"] = us
        sumByUsTable.appendRow(row)

    ctr2Col =
    cvrCol =
    cpm3Col =





    #ordict = dict()
    #ordict["A"] = 1
    #ordict["B"] = 2
    #ordict["C"] = 3
    #print(ordict.keys())
