package com.fangtang.idataclient.config;

import com.alibaba.fastjson.JSONObject;
import com.fangtang.idataclient.utils.Constant;

import java.io.*;
import java.util.Map;

/**
 * *
 *
 * @Author Mr.JIA
 * @Date 2022/12/12 14:18
 **/
public class ReadConfig {

    public void readJsonFile(String dataLibraryName) throws IOException {
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
        Map<String,Object> databaseInfo = (Map<String, Object>) jsonObject.get(dataLibraryName);
        Constant.serverName = databaseInfo.get("serverName").toString();
        Constant.port = databaseInfo.get("port").toString();
        Constant.databaseName = databaseInfo.get("databaseName").toString();
        Constant.userName = databaseInfo.get("userName").toString();
        Constant.passwd = databaseInfo.get("passwd").toString();
        System.out.println("数据库配置完成");
    }
}
