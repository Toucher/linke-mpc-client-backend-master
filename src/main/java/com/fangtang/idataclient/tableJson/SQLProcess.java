package com.fangtang.idataclient.tableJson;

import com.alibaba.fastjson.JSON;
import com.fangtang.idataclient.utils.JsonUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class SQLProcess {
    public String serverName;
    public String userName;
    public String passwd;
    public String databaseName;
    public Connection connection;
    public Statement statement;

    public String tableName;

    public static String driverName = "com.mysql.cj.jdbc.Driver";

    public TableMetadata[] tableMetadatas;

    public SQLProcess(String serverName, String databaseName, String userName, String passwd) {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        this.serverName = serverName;
        this.databaseName = databaseName;
        this.userName = userName;
        this.passwd = passwd;
        List<TableMetadata> tableMetadataList = new ArrayList<>();

        String url = "jdbc:mysql://" + serverName + "/" + databaseName;
        try {
            connection = DriverManager.getConnection(url, userName, passwd);
            DatabaseMetaData databaseMetaData = connection.getMetaData();

            ResultSet resultSet = databaseMetaData.getTables(null, null, null, new String[]{"TABLE"});
            while (resultSet.next()) {
                String tableName = resultSet.getString("TABLE_NAME");
                String tableSchem = resultSet.getString("TABLE_SCHEM");
                String tableCat = resultSet.getString("TABLE_CAT");
                if (tableCat.equals(databaseName)) {
                    TableMetadata tableMetadata = new TableMetadata(databaseMetaData, tableName, tableSchem, tableCat);
                    tableMetadataList.add(tableMetadata);
                }
                tableMetadatas = new TableMetadata[tableMetadataList.size()];
                tableMetadataList.toArray(tableMetadatas);
            }
            statement = connection.createStatement();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public SQLProcess(String serverName, String databaseName, String tableName, String userName, String passwd) {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        this.serverName = serverName;
        this.databaseName = databaseName;
        this.userName = userName;
        this.passwd = passwd;
        List<TableMetadata> tableMetadataList = new ArrayList<>();

        String url = "jdbc:mysql://" + serverName + "/" + databaseName;
        try {
            connection = DriverManager.getConnection(url, userName, passwd);
            DatabaseMetaData databaseMetaData = connection.getMetaData();

            //ResultSet resultSet = databaseMetaData.getTables(null, null, null, new String[] { "TABLE" });
//            while(resultSet.next()){
            String tableSchem = null;
            String tableCat = databaseName;
            if (tableCat.equals(databaseName)) {
                TableMetadata tableMetadata = new TableMetadata(databaseMetaData, tableName, tableSchem, tableCat);
                tableMetadataList.add(tableMetadata);
            }
            tableMetadatas = new TableMetadata[tableMetadataList.size()];
            tableMetadataList.toArray(tableMetadatas);
            //System.out.println(Arrays.toString(tableMetadatas));
//            }
            statement = connection.createStatement();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void createDatabase(String serverName, String databaseName, String userName, String passwd) {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
//        List<TableMetadata> tableMetadataList = new ArrayList<>();

        String url = "jdbc:mysql://" + serverName + "/";
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(url, userName, passwd);
            Statement statementTmp = connection.createStatement();

            String sqlDrop = "drop database if exists " + databaseName;
            statementTmp.executeUpdate(sqlDrop);
            String sql = "create database " + databaseName;
            statementTmp.executeUpdate(sql);
            statementTmp.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static String[] showTables(String serverName, String databaseName, String userName, String passwd) {
        List<String> Tables = new ArrayList<>();
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        String url = "jdbc:mysql://" + serverName + "/" + databaseName;
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(url, userName, passwd);
            Statement statementTmp = connection.createStatement();

            String sql = "show tables";
            ResultSet queryResult = statementTmp.executeQuery(sql);

            System.out.println("show tables:");
            while (queryResult.next()) {
                System.out.println(queryResult.getString(1));
                Tables.add(queryResult.getString(1));
            }

            statementTmp.close();

            return Tables.toArray(new String[0]);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void databaseTables2JSON(String serverName, String databaseName, String userName, String passwd) {
        List<String> Tables = new ArrayList<>();
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        String url = "jdbc:mysql://" + serverName + "/" + databaseName;
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(url, userName, passwd);
            Statement statementTmp = connection.createStatement();

            String sql = "show tables";
            ResultSet queryResult = statementTmp.executeQuery(sql);

            System.out.println("show tables:");
            while (queryResult.next()) {
                String tableName = queryResult.getString(1);
                TableJson table = new TableJson(serverName, databaseName, tableName, userName, passwd);
                String tableJSONString = JSON.toJSONString(table);
                System.out.println(tableJSONString);
                JsonUtils.bean2JsonFile(table, "D:\\Tables\\" + tableName + ".json");
            }

            statementTmp.close();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static String createTableJson(String serverName, String databaseName, String userName, String passwd,String tableName1) {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        String url = "jdbc:mysql://" + serverName + "/" + databaseName;
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(url, userName, passwd);
            Statement statementTmp = connection.createStatement();

            String sql = "show tables";
            ResultSet queryResult = statementTmp.executeQuery(sql);
            String tableJSONString = "";
            System.out.println("show tables:");
            while (queryResult.next()) {
                String tableName = queryResult.getString(1);
                if (tableName.equals(tableName1)){
                    TableJson table = new TableJson(serverName, databaseName, tableName, userName, passwd);
                    tableJSONString = JSON.toJSONString(table);
                }
            }

            statementTmp.close();
            return tableJSONString;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void dropDatabase(String serverName, String databaseName, String userName, String passwd) {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
//        List<TableMetadata> tableMetadataList = new ArrayList<>();

        String url = "jdbc:mysql://" + serverName + "/";
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(url, userName, passwd);
            String sql = "drop database if exists " + databaseName;
            Statement statementTmp = connection.createStatement();
            statementTmp.executeUpdate(sql);
            statementTmp.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /*
    public void readTableColumns(String tableName, String columnName) {
        String queryString = "select " + columnName + " from " + tableName;
        try {
            ResultSet queryResult = statement.executeQuery(queryString);
            while (queryResult.next()) {
                String record = queryResult.getString(columnName);
                //System.out.println(record);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
     */

    public List<String[]> readTable(String tableName, Column[] columns) {
        List<String[]> retPayload = new ArrayList<>();

        StringBuilder columnsStringBuilder = new StringBuilder();
        for (int i = 0; i < columns.length; i++) {
            columnsStringBuilder.append(columns[i].getColumnName());
            if (i < columns.length - 1) {
                columnsStringBuilder.append(", ");
            }
        }

        String queryString = "select " + columnsStringBuilder + " from " + tableName;
        try {
            ResultSet queryResult = statement.executeQuery(queryString);
            int counter;
            while (queryResult.next()) {
                String[] rowPayload = new String[columns.length];
                counter = 0;
                for (Column column : columns) {
                    String record = queryResult.getString(column.getColumnName());
                    rowPayload[counter] = record;
                    //System.out.println(record);
                    counter += 1;
                }
                retPayload.add(rowPayload);
            }
            return retPayload;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public void writeTable(String tableName, Column[] columns) {
        String createTableQuery = "create table " + tableName + "(";

        for (int i = 0; i < columns.length; i++) {
            createTableQuery += columns[i].getColumnName() + " " + columns[i].getColumnType().columnToMySQLType(true);
            if (i < columns.length - 1)
                createTableQuery += ",";
        }

        createTableQuery += ")";
        try {
            statement.executeUpdate(createTableQuery);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        try {
            connection.close();
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public TableMetadata[] getTableMetadatas() {
        return tableMetadatas;
    }

    public static ColumnType mysqlType2ColType(String mysqlType) {
        String lowMysqlType = mysqlType.toLowerCase(Locale.ROOT);
        switch (lowMysqlType) {
            case "int":
                return ColumnType.INT;
            case "tinyint":
                return ColumnType.INT;
            case "smallint":
                return ColumnType.INT;
            case "mediumint":
                return ColumnType.INT;
            case "integer":
                return ColumnType.INT;
            case "double":
                return ColumnType.LONGDECIMAL;

            case "bigint":
                return ColumnType.LONG;

            case "float":
                return ColumnType.DECIMAL;

            case "char":
                return ColumnType.STRING;
            case "varchar":
                return ColumnType.STRING;
            case "tinyblob":
                return ColumnType.STRING;
            case "tinytext":
                return ColumnType.STRING;
            case "blob":
                return ColumnType.STRING;
            case "text":
                return ColumnType.STRING;
            case "mediumblob":
                return ColumnType.STRING;
            case "mediumtext":
                return ColumnType.STRING;
            case "longblob":
                return ColumnType.STRING;
            case "longtext":
                return ColumnType.STRING;


            case "bit":
                return ColumnType.BIT;

            case "date":
                return ColumnType.DATE;

            case "timestamp":
                return ColumnType.TIMESTAMP;

            default:
                return null;

        }
    }
}

