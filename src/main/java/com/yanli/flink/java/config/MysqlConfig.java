package com.yanli.flink.java.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author YanLi
 * @version 1.0
 * @ClassName: MysqlConfig
 * @date 2021/2/24 5:08 下午
 * 获取mysql连接配置信息
 */
public class MysqlConfig {

    public static String DRIVER_CLASS;
    public static String DB_URL;
    public static String USER_NAME;
    public static String PASSWORD;

    static{
        Properties properties = new Properties();
        InputStream resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("mysql.properties");
        try {
            properties.load(resourceAsStream);
            DRIVER_CLASS = properties.getProperty("driverClass");
            DB_URL = properties.getProperty("dbUrl");
            USER_NAME = properties.getProperty("userNmae");
            PASSWORD = properties.getProperty("passWord");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
