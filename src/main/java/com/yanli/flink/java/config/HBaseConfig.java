package com.yanli.flink.java.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author YanLi
 * @version 1.0
 * @ClassName: HbaseConfig
 * @date 2021/2/24 5:08 下午
 * 获取hbase config
 */
public class HBaseConfig {
    public static final String ROW_KEY = "rowKey";
    public static final String COLUMN_NAME_LIST = "columnNameList";

    public static Configuration getHBaseConfig(String zookeeperQuorum){
        Properties properties = new Properties();
        InputStream resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("hbase.properties");
        try {
            properties.load(resourceAsStream);
        } catch (IOException e) {
            e.printStackTrace();
        }

        Configuration configuration = HBaseConfiguration.create();
        configuration.set(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum);
        configuration.set(HConstants.ZOOKEEPER_CLIENT_PORT, properties.getProperty("hbase.zookeeper.property.clientPort"));
        configuration.set(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, properties.getProperty("hbase.client.operation.timeout"));
        configuration.set(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, properties.getProperty("hbase.client.scanner.timeout.period"));

        return configuration;
    }
}
