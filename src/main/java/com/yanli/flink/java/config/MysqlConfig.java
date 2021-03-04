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
    public static Properties getMysqlConfig(){
        Properties properties = new Properties();
        InputStream resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("mysql.properties");
        try {
            properties.load(resourceAsStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }
}
