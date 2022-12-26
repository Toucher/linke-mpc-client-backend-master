package com.fangtang.idataclient.utils;

import com.alibaba.fastjson.JSON;
import com.fangtang.idataclient.tableJson.TableJson;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * *
 *
 * @Author Mr.JIA
 * @Date 2022/12/11 9:33
 **/
public class JsonUtil {

    public static String driverName = "com.mysql.cj.jdbc.Driver";

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
                JsonUtils.bean2JsonFile(table, "Tables/" + tableName + ".json");
            }

            statementTmp.close();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
