package com.fangtang.idataclient.utils;

import com.fangtang.idataclient.tableJson.Table;
import com.fangtang.idataclient.tableJson.TableColumn;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * *
 *
 * @Author Mr.JIA
 * @Date 2022/12/10 10:57
 **/
public class ConstructionToWord {

    private final String DRIVER = "com.mysql.cj.jdbc.Driver";
    //private final String DRIVER = "com.mysql.cj.jdbc.Driver";


    Connection conn = null;
    PreparedStatement pst = null;
    ResultSet rs = null;

    // 获取查询数据
    public ArrayList<Map<String,String>> getData() throws Exception{

        System.out.println("数据生成中，请稍等...");
        ArrayList<Map<String,String>> tableList = new ArrayList<>();
        List<Table> tables = getTables(Constant.databaseName);

        for (Table table : tables) {
            Map<String,String> map = new HashMap<String,String>();
            List<TableColumn> columns = getColumns(Constant.databaseName,table.getTableName());
            String columnStr = "";
            for (int i = 0; i < columns.size(); i++) {
                if (i == columns.size() - 1){
                    columnStr += columns.get(i).getColumnName();
                }else {
                    columnStr += columns.get(i).getColumnName() + "#";
                }
            }
            map.put("tableName",table.getTableName());
            map.put("tableField",columnStr);
            tableList.add(map);
        }
        return tableList;
    }


    // 获取表字段信息
    public List<TableColumn>  getColumns(String database,String tableName) throws Exception{

        String sql = "select column_name,column_comment,column_type,is_nullable, column_key from information_schema.columns  where  table_schema=? and table_name=?";
        ResultSet rs = getConn(database,tableName,sql);

        List<TableColumn> tableColumns = new ArrayList<TableColumn>();

        while (rs.next()){
            TableColumn tc = new TableColumn();
            tc.setTableName(tableName);
            tc.setColumnName(rs.getString("column_name"));
            tc.setColumnType(rs.getString("column_type"));
            tc.setColumnKey(rs.getString("column_key"));
            tc.setIsNullable(rs.getString("is_nullable"));
            tc.setColumnComment(rs.getString("column_comment"));
            tableColumns.add(tc);
        }

        releaseConn();

        return tableColumns;

    }


    // 获取所有表
    public List<Table> getTables(String database) throws Exception{

        String  sql = "select table_name,table_comment from information_schema.tables where table_schema=?";
        ResultSet rs = getConn(database, "",sql);

        List<Table> tables = new ArrayList<Table>();
        while(rs.next()){
            Table table = new Table();
            table.setTableName(rs.getString( "table_name"));
            table.setTableCommont(rs.getString("table_comment"));
            tables.add(table);
        }

        releaseConn();
        return  tables;

    }

    // 连接数据库
    private ResultSet getConn(String dataBase, String tableName, String sql){

        try{
            String URL = "jdbc:mysql://"+ Constant.serverName+":"+ Constant.port+"/"+dataBase+"?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=utf8";
            Class.forName(DRIVER);
            conn = DriverManager.getConnection(URL, Constant.userName, Constant.passwd);
            pst = conn.prepareStatement(sql);
            pst.setString(1,dataBase);
            if(!"".equals(tableName)){
                pst.setString(2,tableName);
            }
            rs = pst.executeQuery();
            return  rs;

        }catch (Exception e){
            e.printStackTrace();
        }

        return null;

    }

    // 释放连接
    private void  releaseConn(){
        try{
            if(rs != null ){
                rs.close();
            }
            if(pst != null){
                pst.close();
            }
            if(conn != null){
                conn.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}
