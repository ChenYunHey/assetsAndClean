package com.lakesoul.assets;

public class TableInfo {
    String tableId;
    String tableName;
    String domain;
    String creator;
    String nameSpace;

    public TableInfo(String tableId, String tableName, String domain, String creator, String nameSpace) {
        this.tableId = tableId;
        this.tableName = tableName;
        this.domain = domain;
        this.creator = creator;
        this.nameSpace = nameSpace;
    }

    @Override
    public String toString() {
        return "TableInfo{" +
                "tableId='" + tableId + '\'' +
                ", tableName='" + tableName + '\'' +
                ", domain='" + domain + '\'' +
                ", creator='" + creator + '\'' +
                ", nameSpace='" + nameSpace + '\'' +
                '}';
    }
}
