package com.yanli.flink.java.streamingApi.mysql;

import com.yanli.flink.java.config.MysqlConfig;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat.JDBCInputFormatBuilder;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat.JDBCOutputFormatBuilder;


/**
 * @author YanLi
 * @version 1.0
 * @ClassName: FlinkJDBCFormat
 * @date 2021/2/24 5:13 下午
 */
@Deprecated
public class FlinkJDBCFormat {


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
                .setDrivername(MysqlConfig.DRIVER_CLASS)
                .setDBUrl(MysqlConfig.DB_URL)
                .setUsername(MysqlConfig.USER_NAME)
                .setPassword(MysqlConfig.PASSWORD);

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
                .setDrivername(MysqlConfig.DRIVER_CLASS)
                .setDBUrl(MysqlConfig.DB_URL)
                .setUsername(MysqlConfig.USER_NAME)
                .setPassword(MysqlConfig.PASSWORD);

        return outputFormatBuilder;
    }
}
