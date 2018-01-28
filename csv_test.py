import CsvTables as cs
def baseConstructTest(table):
    print(table)
    print(table.storage)
    print(table.colNameIdxMap)
    print(table.getShape())
    table.asType("value", float)
    table.asType("id", int)
    print(table.storage)

if __name__ == "__main__":
    table = cs.DataTable("nohead.csv", columnsNames=["id", "name", "value", "country"])
    # table = cs.DataTable(dataMat={"id":[1, 2, 3, 4], "name":['a', 'b', 'c', 'd'], 'value':['100.0', '200.0', '300.0', '400.0']})
    #table.toCsv("out.csv", ignoreColName=True)
    #print(table)
    #print(table.get(0, "name"))
    #print(table.get(1, "value"))
    #print(table.get(2, "country"))

    #table.set(1, "country", "GER")
    #print(table)
    #print(table.getColumns("name"))
    #print(table.getColumns(["country", "name"]))
    #baseConstructTest(table)
    #print(table)
    #print(table.get_1Row(0))
    #print(table.getRows(2, 4))
    #table.appendRow({"country":"AUS", "id": 5, "value": 340.0, "name":"zhao"})
    #print(table)
    #print("-----------")
    #table.asType("id", int)
    #print(table.filterOutRows(lambda r: r["id"] > 2))
    #table.appendColumn({"cash":[101, 102, 103, 104, 105]})
    #print(table)
    ##print(type([None,]*5))
    #print(table.storage)
    #table.asType("value", float)
    #print(table.mapByRow(lambda r: (r["id"] + r["cash"], r["country"] + "|" + r["name"])))
    #print(table.mapByRow(lambda r: dict([("sum", r["id"] + r["cash"]), ("concat", r["country"] + "|" + r["name"])]), mapMethod='row', newColumnsNames=["sum", "concat"]))
    groupObj = table.groupBy(["country", "name"])
    for k in groupObj:
        print(k)
        print(groupObj[k])
    #print()



