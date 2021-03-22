package com.yanli.flink.java.streamingApi.mysql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @author YanLi
 * @version 1.0
 * @ClassName: SampleAsyncDatabase
 * @date 2021/3/19 5:29 下午
 * 通过异步IO读取外部数据库数据
 */
public class SampleAsyncDatabase extends RichAsyncFunction<String, Tuple2<String, String>>{

    private transient Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.jdbc.Driver");
        connection = DriverManager.getConnection("url", "user", "password");
        connection.setAutoCommit(false);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<Tuple2<String, String>> resultFuture) throws Exception {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("sql");


    }

    @Override
    public void timeout(String input, ResultFuture<Tuple2<String, String>> resultFuture) throws Exception {
        super.timeout(input,resultFuture);
    }
}
