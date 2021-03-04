package com.yanli.flink.java.streamingApi.mysql;

import com.yanli.flink.java.config.MysqlConfig;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat.JDBCInputFormatBuilder;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat.JDBCOutputFormatBuilder;

import java.util.Properties;

/**
 * @author YanLi
 * @version 1.0
 * @ClassName: FlinkJDBCFormat
 * @date 2021/2/24 5:13 下午
 */
public class FlinkJDBCFormat {
    private static Properties properties  = MysqlConfig.getMysqlConfig();

    /**
     * 功能描述:
     * 〈创建JDBCInputFormatBuilder〉
     * @Param: []
     * @Return: org.apache.flink.api.java.io.jdbc.JDBCInputFormat.JDBCInputFormatBuilder
     * @Author: yanli
     * @Date: 2021/2/22 11:32 上午
     */
    public static JDBCInputFormatBuilder getJDBCInputFormat(){
        JDBCInputFormatBuilder inputFormatBuilder = JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername(properties.getProperty("driverClass"))
                .setDBUrl(properties.getProperty("dbUrl"))
                .setUsername(properties.getProperty("userNmae"))
                .setPassword(properties.getProperty("passWord"));

        return inputFormatBuilder;
    }

    /**
     * 功能描述:
     * 〈创建JDBCOutputFormatBuilder〉
     * @Param: []
     * @Return: org.apache.flink.api.java.io.jdbc.JDBCOutputFormat.JDBCOutputFormatBuilder
     * @Author: yanli
     * @Date: 2021/2/22 11:32 上午
     */
    public static JDBCOutputFormatBuilder getJDBCOutputFormat(){
        JDBCOutputFormatBuilder outputFormatBuilder = JDBCOutputFormat.buildJDBCOutputFormat()
                .setDrivername(properties.getProperty("driverClass"))
                .setDBUrl(properties.getProperty("dbUrl"))
                .setUsername(properties.getProperty("userNmae"))
                .setPassword(properties.getProperty("passWord"));

        return outputFormatBuilder;
    }
}
