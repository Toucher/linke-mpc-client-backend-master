package com.fangtang.idataclient.tableJson;

/**
 * *
 *  表列信息
 * @Author Mr.JIA
 * @Date 2022/12/10 11:44
 **/
public class TableColumn {

    // 表名
    private String tableName;
    // 字段名
    private String columnName;
    // 字段类型
    private String columnType;
    // 字段注释
    private String columnComment;
    // 可否为空
    private String isNullable;
    // 约束
    private String columnKey;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnType() {
        return columnType;
    }

    public void setColumnType(String columnType) {
        this.columnType = columnType;
    }

    public String getColumnComment() {
        return columnComment;
    }

    public void setColumnComment(String columnComment) {
        this.columnComment = columnComment;
    }

    public String getIsNullable() {
        return isNullable;
    }

    public void setIsNullable(String isNullable) {
        this.isNullable = isNullable;
    }

    public String getColumnKey() {
        return columnKey;
    }

    public void setColumnKey(String columnKey) {
        this.columnKey = columnKey;
    }
}
