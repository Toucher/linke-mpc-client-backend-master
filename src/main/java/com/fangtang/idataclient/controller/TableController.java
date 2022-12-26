package com.fangtang.idataclient.controller;

import cn.hutool.http.HttpRequest;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.fangtang.idataclient.config.ReadConfig;
import com.fangtang.idataclient.tableJson.SQLProcess;
import com.fangtang.idataclient.utils.ConstructionToWord;
import com.fangtang.idataclient.utils.JsonUtils;
import io.swagger.annotations.ApiOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * *
 * 请求表结构信息
 * @Author Mr.JIA
 * @Date 2022/12/10 12:00
 **/
@RestController
public class TableController {
    private final static Logger logger = LoggerFactory.getLogger(TableController.class);
    @Value("${backend.uri}")
    private String backendUri;
    @Value("${task.basedir}")
    private String taskBaseDir;

    @ApiOperation(value = "获取表结构")
    @GetMapping("/getTableStructure")
    public String getTableStructure(HttpServletRequest request) throws IOException {
        String dataLibraryName = request.getParameter("name");
        logger.info("dataLibraryName===>" + dataLibraryName);
        ReadConfig readConfig = new ReadConfig();
        readConfig.readJsonFile(dataLibraryName);
        JSONObject jsonObject = new JSONObject();
        try {
            ConstructionToWord rd = new ConstructionToWord();
            ArrayList<Map<String, String>> data = rd.getData();
            jsonObject.put("code", 200);
            jsonObject.put("data", data);
            logger.info("返回表结构===>" + jsonObject.toString());
            return jsonObject.toString();
        } catch (Exception e) {
            logger.error("获取表结构异常：" + e.toString());
            jsonObject.put("code", 201);
            jsonObject.put("data", "");
            return jsonObject.toString();
        }
    }

    @ApiOperation(value = "校验数据库连接")
    @GetMapping("/checkTableStructure")
    public String checkTableStructure(HttpServletRequest request) throws IOException {
        String dataLibraryName = request.getParameter("name");
        logger.info("dataLibraryName===>" + dataLibraryName);
        ReadConfig readConfig = new ReadConfig();
        readConfig.readJsonFile(dataLibraryName);
        try {
            ConstructionToWord rd = new ConstructionToWord();
            ArrayList<Map<String, String>> data = rd.getData();
            return "200";
        } catch (Exception e) {
            logger.error("获取表结构异常：" + e.toString());
            return "201";
        }
    }

    @ApiOperation(value = "计算方法")
    @PostMapping("/clientComputing")
    public String clientComputing(@RequestBody Map<String, Object> map) throws Exception {
        logger.info("-----------开始读取本地数据库配置----------------");
        String taskId = (String) map.get("taskId");
        String taskDir = taskBaseDir + taskId;
        String filePath = taskDir + "/testInput/tableJson/";
        File saveFile = new File(filePath);
        if (!saveFile.exists()) {
            saveFile.mkdirs();
        }
        String filePath1 = taskDir + "/testInput/plan/";
        File saveFile1 = new File(filePath1);
        if (!saveFile1.exists()) {
            saveFile1.mkdirs();
        }
        String logStr = "log4j.rootLogger = INFO,consoleAppender,logfile\n";
        logStr += "log4j.appender.consoleAppender = org.apache.log4j.ConsoleAppender\n";
        logStr += "log4j.appender.consoleAppender.layout = org.apache.log4j.PatternLayout\n";
        logStr += "log4j.appender.consoleAppender.layout.ConversionPattern = %-d{yyyy-MM-dd HH:mm:ss,SSS} [%l]-[%p] %m%n\n";
        logStr += "log4j.appender.logfile = org.apache.log4j.DailyRollingFileAppender\n";
        logStr += "log4j.appender.logfile.File = " + taskDir + "/log.log\n";
        logStr += "log4j.appender.logfile.Append = false\n";
        logStr += "log4j.appender.logfile.DatePattern = '.'yyyy-MM-dd\n";
        logStr += "log4j.appender.logfile.layout = org.apache.log4j.PatternLayout\n";
        logStr += "log4j.appender.logfile.layout.ConversionPattern = %d{yyyy-MM-dd HH:mm:ss,SSS} [%t] [%c] [%p] - %m%n";
        JsonUtils.bean3JsonFile(logStr, taskDir + "/log4j.properties");
        logger.info("封装任务编号："+taskId);

        String databaseName1 = (String) map.get("databaseName");
        File file = new File("./dataBase.json");
        FileReader fileReader = new FileReader(file);
        Reader reader = new InputStreamReader(new FileInputStream(file), "Utf-8");
        int ch = 0;
        StringBuffer sb = new StringBuffer();
        while ((ch = reader.read()) != -1) {
            sb.append((char) ch);
        }
        fileReader.close();
        reader.close();
        String jsonStr = sb.toString();
        //System.out.println(JSON.parseObject(jsonStr));
        JSONObject jsonObject = JSONObject.parseObject(jsonStr);
        Map<String, Object> databaseInfo = (Map<String, Object>) jsonObject.get(databaseName1);
        String serverName = databaseInfo.get("serverName").toString();
        String port = databaseInfo.get("port").toString();
        String databaseName = databaseInfo.get("databaseName").toString();
        String userName = databaseInfo.get("userName").toString();
        String passwd = databaseInfo.get("passwd").toString();
        HashMap<String, Object> testParam = (HashMap<String, Object>) map.get("testParam");
        logger.info("-----------开始封装testParam.json文件数据----------------");
        testParam.put("task_name", taskId);
        testParam.put("sql_parse_result", taskDir + "/testInput/plan/MySQL_plan.json");
        testParam.put("database_metadata_json_dir", taskDir + "/testInput/tableJson");
        testParam.put("my_name", map.get("companyName").toString());
        testParam.put("my_database_name", databaseName);
        testParam.put("my_server_name", serverName + ":" + port);
        testParam.put("my_database_userName", userName);
        testParam.put("my_database_password", passwd);
        testParam.put("isOutputParty", map.get("isOutputParty").toString());
        testParam.put("isInputParty", map.get("isInputParty").toString());
        testParam.put("compute_role", map.get("order").toString());
        testParam.put("isComputeParty", map.get("isComputeParty").toString());
        logger.info("testParam===>" + JSON.toJSONString(testParam));
        logger.info("-----------结束封装testParam.json文件数据----------------");
        JsonUtils.bean3JsonFile(JSON.toJSONString(testParam), taskDir + "/testInput/testParam.json");

        logger.info("-----------开始封装保存MySQL_plan.json文件内容----------------");
        String config = (String) map.get("config");
        ArrayList<Map<String, String>> tableList = (ArrayList<Map<String, String>>) map.get("tableList");

        JsonUtils.bean3JsonFile(config, taskDir + "/testInput/plan/MySQL_plan.json");
        logger.info("-----------结束封装保存MySQL_plan.json文件内容----------------");
        //JsonUtils.bean3JsonFile(config,"D:\\"+taskId+"\\config.json");
        logger.info("-----------开始封装表格.json文件内容----------------");
        for (Map<String, String> stringStringMap : tableList) {
            JsonUtils.bean3JsonFile(stringStringMap.get("jsonStr"), filePath + stringStringMap.get("tableName") + ".json");
        }
        logger.info("-----------开始封装表格.json文件内容----------------");
        return "200";
    }

    @ApiOperation(value = "运行客户端jar包")
    @PostMapping("/runClientJarAndGetResult")
    public String runClientJarAndGetResult(@RequestBody Map<String,Object> map) throws IOException {
        String taskId = map.get("taskId").toString();
        String taskDir = taskBaseDir + taskId;
        //需要将java17放到路径中
        String shText = "java -javaagent:db3-1.0-SNAPSHOT-jar-with-dependencies-encrypted.jar='-pwd iiwcadncts23' -jar db3-1.0-SNAPSHOT-jar-with-dependencies-encrypted.jar " + taskDir + "/testInput/testParam.json " + taskDir + "/log4j.properties > db3_" + taskId + ".log 2>&1 &";
        Files.write(Paths.get("./start_db3.sh"), Arrays.asList("#!/bin/bash", shText), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        File file = new File("start_db3.sh");
        file.setExecutable(true);
        logger.info("shText："+shText);
        logger.info("-----------开始运行jar----------------");
        Process exec = Runtime.getRuntime().exec("./start_db3.sh");
        logger.info("-----------jar运行结束----------------");
        logger.info("-----------开始读取日志文件----------------");

        try {
            exec.waitFor();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        int tryTimes = 50;
        while (true) {
            File logFile = new File(taskDir + "/log.log");
            if (logFile.exists()) {
                break;
            }

            logger.info("{} not exists, try after 2s. {} times left to try", logFile, tryTimes);
            try {
                Thread.sleep(2000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        ArrayList<String> newLogList = new ArrayList<>();
        boolean flag = false;
        boolean stopFor = false;
        for (int i = 0; i < 30; i++) {
            logger.info("执行到第"+i+"次");
            if (i == 29) {
                flag = true;
            }
            ArrayList<String> logList = new ArrayList<>();
            try {
                String MySQL_planfpath = taskDir + "/log.log";
                File MySQL_planfile = new File(MySQL_planfpath);
                if (MySQL_planfile.exists()) {
                    try (FileReader reader = new FileReader(MySQL_planfile)) {
                        try (BufferedReader br = new BufferedReader(reader)) {
                            String MySQL_planstr_line;
                            //逐行读取文本
                            while ((MySQL_planstr_line = br.readLine()) != null) {
                                logList.add(MySQL_planstr_line);
                            }
                        }
                    }
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            logger.info("日志长度：" + logList.size());
            ArrayList<String> sendLogList = new ArrayList<>();
            if (newLogList.size() > 0) {
                logger.info("newLogList====>"+newLogList.size());
                logger.info("logList====>"+logList.size());
                if (logList.size() != newLogList.size()) {
                    int num = logList.size() - newLogList.size();
                    for (int j = 0; j < num; j++) {
                        sendLogList.add(logList.get(newLogList.size() + j));
                    }
                    newLogList = logList;
                }
            } else {
                logger.info("newLogList第一次进====>"+newLogList.size());
                newLogList = logList;
                sendLogList = logList;
                logger.info("newLogList第一次出====>"+newLogList.size());
            }
            logger.info("--------------------sendLogList----------------------"+sendLogList.size());
            if (sendLogList.size() > 0) {
                Map<String, Object> sendInfo = new HashMap<>();
                sendInfo.put("companyId", map.get("companyId").toString());
                sendInfo.put("taskId", taskId);
                sendInfo.put("logs", sendLogList);
                String result1 =
                        HttpRequest.post(backendUri + "/task/saveClientLog")
                                .body(JSON.toJSONString(sendInfo), "application/json")
                                .execute()
                                .body();
                logger.info("save client log res {}", result1);
                for (String log : sendLogList) {
                    logger.info("log====>"+log);
                    logger.info("匹配结果====>"+log.contains("自己的任务已经完成了"));
                    if (log.contains("自己的任务已经完成了")){
                        ArrayList<String> list = new ArrayList<>();
                        list.add("执行完成");
                        sendInfo.put("logs", list);
                        String result2 =
                                HttpRequest.post(backendUri + "/task/saveClientLog")
                                        .body(JSON.toJSONString(sendInfo), "application/json")
                                        .execute()
                                        .body();
                        stopFor = true;
                    }
                }
            }
            if (stopFor){
                break;
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        logger.info("-----------结束读取日志文件----------------");
        if (flag) {
            ArrayList<String> errLogs = new ArrayList<>();
            errLogs.add("日志获取错误");
            Map<String, Object> sendInfo = new HashMap<>();
            sendInfo.put("logs", errLogs);
            sendInfo.put("companyId", map.get("companyId").toString());
            sendInfo.put("taskId", taskId);
            String result1 =
                    HttpRequest.post(backendUri + "/task/saveClientLog")
                            .body(JSON.toJSONString(sendInfo), "application/json")
                            .execute()
                            .body();
        }
        return "运行客户端jar包成功";
    }

    @ApiOperation(value = "获取表格.json文件")
    @GetMapping("/getTableJson/{dataBaseName}/{tableName}")
    public String getTableJson(@PathVariable("dataBaseName") String dataBaseName, @PathVariable("tableName") String tableName) throws Exception {
        System.out.println("请求到接口");
        File file = new File("./dataBase.json");
        FileReader fileReader = new FileReader(file);
        Reader reader = new InputStreamReader(new FileInputStream(file), "Utf-8");
        int ch = 0;
        StringBuffer sb = new StringBuffer();
        while ((ch = reader.read()) != -1) {
            sb.append((char) ch);
        }
        fileReader.close();
        reader.close();
        String jsonStr = sb.toString();
        //System.out.println(JSON.parseObject(jsonStr));
        JSONObject jsonObject = JSONObject.parseObject(jsonStr);
        Map<String, Object> databaseInfo = (Map<String, Object>) jsonObject.get(dataBaseName);
        String serverName = databaseInfo.get("serverName").toString();
        String port = databaseInfo.get("port").toString();
        String databaseName = databaseInfo.get("databaseName").toString();
        String userName = databaseInfo.get("userName").toString();
        String passwd = databaseInfo.get("passwd").toString();
        String tableJson = SQLProcess.createTableJson(serverName + ":" + port, databaseName, userName, passwd, tableName);
        return tableJson;
    }

    @ApiOperation(value = "获取数据库配置信息")
    @PostMapping("/getDatabaseInfo")
    public String getDatabaseInfo(@RequestBody Map<String, String> map) throws Exception {
        System.out.println("请求到接口");
        String dataBaseName = map.get("dataBaseName");
        File file = new File("./dataBase.json");
        FileReader fileReader = new FileReader(file);
        Reader reader = new InputStreamReader(new FileInputStream(file), "Utf-8");
        int ch = 0;
        StringBuffer sb = new StringBuffer();
        while ((ch = reader.read()) != -1) {
            sb.append((char) ch);
        }
        fileReader.close();
        reader.close();
        String jsonStr = sb.toString();
        //System.out.println(JSON.parseObject(jsonStr));
        JSONObject jsonObject = JSONObject.parseObject(jsonStr);
        Map<String, Object> databaseInfo = (Map<String, Object>) jsonObject.get(dataBaseName);
        String serverName = databaseInfo.get("serverName").toString();
        String port = databaseInfo.get("port").toString();
        String databaseName = databaseInfo.get("databaseName").toString();
        String userName = databaseInfo.get("userName").toString();
        String passwd = databaseInfo.get("passwd").toString();
        String dataBaseInfo = serverName + "_" + port + "_" + databaseName;
        return dataBaseInfo;
    }

    @ApiOperation(value = "获取jar包运行后结果")
    @PostMapping("/getResult")
    @CrossOrigin
    public String getResult(@RequestBody Map<String,Object> map) throws Exception {
        String taskId = (String) map.get("taskId");
        Class.forName("com.mysql.cj.jdbc.Driver");
        File file = new File("./dataBase.json");
        FileReader fileReader = new FileReader(file);
        Reader reader = new InputStreamReader(new FileInputStream(file), "Utf-8");
        int ch = 0;
        StringBuffer sb = new StringBuffer();
        while ((ch = reader.read()) != -1) {
            sb.append((char) ch);
        }
        fileReader.close();
        reader.close();
        String jsonStr = sb.toString();
        //System.out.println(JSON.parseObject(jsonStr));
        JSONObject jsonObject = JSONObject.parseObject(jsonStr);
        Map<String, Object> databaseInfo = (Map<String, Object>) jsonObject.get("result_database");
        String serverName = databaseInfo.get("serverName").toString();
        String port = databaseInfo.get("port").toString();
        String databaseName = databaseInfo.get("databaseName").toString();
        String userName = databaseInfo.get("userName").toString();
        String passwd = databaseInfo.get("passwd").toString();
        String url = "jdbc:mysql://" + serverName + ":" + port + "/" + databaseName;
        ArrayList<String> dataList = null;
        try (Connection connection = DriverManager.getConnection(url, userName, passwd)) {
            for (int i = 0; i < 7; i++) {
                dataList = new ArrayList<>();
                boolean hasTable = false;
                try (Statement statement = connection.createStatement()) {
                    String sql1 = "show tables";
                    try (ResultSet queryResult = statement.executeQuery(sql1)) {
                        while (queryResult.next()) {
                            String tableName = queryResult.getString(1);
                            logger.info("tableName===>" + tableName);
                            if (taskId.equals(tableName)) {
                                hasTable = true;
                                break;
                            }
                        }
                    }
                }

                if (!hasTable) {
                    logger.info("no table of task {} found, try after 2s", taskId);
                    Thread.sleep(2000);
                }

                String sql = "select * from " + taskId;
                try (Statement statement = connection.createStatement()) {
                    try (ResultSet resultSet = statement.executeQuery(sql)) {
                        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                        int columnCount = resultSetMetaData.getColumnCount();
                        ArrayList<String> columns = new ArrayList<>();
                        for (int j = 1; j <= columnCount; j++) {
                            String columnName = resultSetMetaData.getColumnName(j);
                            columns.add(columnName);
                        }
                        //逐行读取文本
                        while (resultSet.next()) {
                            for (String column : columns) {
                                String context = resultSet.getString(column);
                                dataList.add(context);
                            }
                        }
                        if (!dataList.isEmpty()) {
                            break;
                        }
                    }
                }
                Thread.sleep(2000);
                logger.info("no data of task {} found, try after 2s", taskId);
            }
        }
        JSONObject jsonObject1 = new JSONObject();
        if (dataList.size() > 0){
            jsonObject1.put("code",200);
            jsonObject1.put("message","");
            jsonObject1.put("dataList",dataList);
        }else {
            jsonObject1.put("code",201);
            jsonObject1.put("message","未发现任务对应的结果表");
            jsonObject1.put("dataList","");
        }
        return jsonObject1.toString();
    }
}
