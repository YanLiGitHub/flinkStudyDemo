package com.yanli.flink.java.config;

import com.yanli.flink.java.utils.PropertiesUtil;

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

    public static String CDC_HOST;
    public static Integer CDC_PORT;
    public static String CDC_DATABASE_NAME;
    public static String CDC_TABLE_NAME;
    public static String CDC_USER_NAME;
    public static String CDC_PASSWORD;

    static {
        Properties properties = PropertiesUtil.getProperties("mysql.properties");
        DRIVER_CLASS = properties.getProperty("sink.driverClass");
        DB_URL = properties.getProperty("sink.dbUrl");
        USER_NAME = properties.getProperty("sink.userName");
        PASSWORD = properties.getProperty("sink.passWord");

        CDC_HOST=properties.getProperty("cdc.source.host");
        CDC_PORT=Integer.valueOf(properties.getProperty("cdc.source.port"));
        CDC_DATABASE_NAME = properties.getProperty("cdc.source.database");
        CDC_TABLE_NAME = properties.getProperty("cdc.source.tableName");
        CDC_USER_NAME = properties.getProperty("cdc.source.userName");
        CDC_PASSWORD = properties.getProperty("cdc.source.passWord");
    }
}
