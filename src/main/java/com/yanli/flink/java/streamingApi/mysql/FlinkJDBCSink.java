package com.yanli.flink.java.streamingApi.mysql;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author YanLi
 * @version 1.0
 * @ClassName: FlinkJDBCSink
 * @date 2021/3/19 2:00 下午
 */
public class FlinkJDBCSink {
    private static final Logger logger = LoggerFactory.getLogger(FlinkJDBCSink.class);

    /**
     * 获取mysqlSink
     * @param dataStream
     * @param url
     * @param driverName
     * @param username
     * @param password
     */
    public static void getMysqlSink(DataStream dataStream,String sql, String url, String driverName, String username, String password){
        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions
                .JdbcConnectionOptionsBuilder()
                .withUrl(url)
                .withDriverName(driverName)
                .withUsername(username)
                .withPassword(password)
                .build();

        //具体实现与SQL有关
        JdbcStatementBuilder jdbcStatementBuilder = new JdbcStatementBuilder(){
            @Override
            public void accept(Object o, Object o2) throws Throwable {

            }
        };

        dataStream.addSink(JdbcSink.sink(sql, jdbcStatementBuilder, connectionOptions));
    }

}
