package com.yanli.flink.java.streamingApi.mysql;

import com.yanli.flink.java.config.MysqlConfig;
import com.yanli.flink.java.pojo.mysql.TulingLectureLabel;
import com.yanli.flink.java.utils.JavaJsonUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author YanLi
 * @version 1.0
 * @ClassName: SampleAsyncDatabase
 * @date 2021/3/19 5:29 下午
 * 通过异步IO读取外部数据库数据
 */
public class SampleAsyncDatabase extends RichAsyncFunction<TulingLectureLabel, TulingLectureLabel>{

    private static final Logger logger = LoggerFactory.getLogger(SampleAsyncDatabase.class);

    private transient Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName(MysqlConfig.DRIVER_CLASS);
        connection = DriverManager.getConnection(MysqlConfig.DB_URL, MysqlConfig.USER_NAME, MysqlConfig.PASSWORD);
        connection.setAutoCommit(false);
    }

    @Override
    public void close() throws Exception {
        logger.info("async function for hbase java close ...");
        super.close();
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    public void asyncInvoke(TulingLectureLabel tulingLectureLabel, ResultFuture<TulingLectureLabel> resultFuture) throws Exception {
        String sql = "select lecture_name,app_tree_lkc_id,app_tree_lkc_name,app_tree_know_id,app_tree_know_name,lkc_id,know_linked_list from tuling_lecture_label where lecture_id = ?";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setString(1,tulingLectureLabel.getLectureId());
        ResultSet resultSet = ps.executeQuery();
        if (resultSet.next()){
            tulingLectureLabel.setLectureName(resultSet.getString(1));
            tulingLectureLabel.setAppTreeKnowId(resultSet.getString("app_tree_lkc_id"));
            tulingLectureLabel.setAppTreeLkcName(resultSet.getString(3));
            tulingLectureLabel.setAppTreeKnowId(resultSet.getString(4));
            tulingLectureLabel.setAppTreeKnowName(resultSet.getString(5));
            tulingLectureLabel.setLkcId(resultSet.getString(6));
            tulingLectureLabel.setKnowLinkedList(resultSet.getString(7));
        }
        logger.info("execute query : " + sql + "-2-  return :" + JavaJsonUtil.getJson(tulingLectureLabel));
        List<TulingLectureLabel> list = new ArrayList<>();
        list.add(tulingLectureLabel);
        resultFuture.complete(list);
    }

    @Override
    public void timeout(TulingLectureLabel tulingLectureLabel, ResultFuture<TulingLectureLabel> resultFuture) throws Exception {
        super.timeout(tulingLectureLabel,resultFuture);
    }
}
