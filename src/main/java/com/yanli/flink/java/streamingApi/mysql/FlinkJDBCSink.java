package com.yanli.flink.java.streamingApi.mysql;

import com.yanli.flink.java.config.MysqlConfig;
import com.yanli.flink.java.pojo.mysql.TulingLectureLabel;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;

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
     */
    public static void mysqlSink(DataStream dataStream,String sql){
        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions
                .JdbcConnectionOptionsBuilder()
                .withUrl(MysqlConfig.DB_URL)
                .withDriverName(MysqlConfig.DRIVER_CLASS)
                .withUsername(MysqlConfig.USER_NAME)
                .withPassword(MysqlConfig.PASSWORD)
                .build();

        //具体实现与SQL有关
        JdbcStatementBuilder jdbcStatementBuilder = new JdbcStatementBuilder<TulingLectureLabel>(){
            /**
             *  sql : insert into tuling_lecture_label (lecture_id, lecture_name, app_tree_lkc_id, app_tree_lkc_name, app_tree_know_id, app_tree_know_name,lkc_id,know_linked_list)
             *          values (?,?,?,?,?,?,?,?)
             * @param ps
             * @param tulingLectureLabel
             * @throws SQLException
             */
            @Override
            public void accept(PreparedStatement ps, TulingLectureLabel tulingLectureLabel) throws SQLException {
                ps.setString(1,tulingLectureLabel.getLectureId());
                ps.setString(2,tulingLectureLabel.getLectureName());
                ps.setString(3,tulingLectureLabel.getAppTreeLkcId());
                ps.setString(4,tulingLectureLabel.getAppTreeLkcName());
                ps.setString(5,tulingLectureLabel.getAppTreeKnowId());
                ps.setString(6,tulingLectureLabel.getAppTreeKnowName());
                ps.setString(7,tulingLectureLabel.getLkcId());
                ps.setString(8,tulingLectureLabel.getKnowLinkedList());
            }
        };

        dataStream.addSink(JdbcSink.sink(sql, jdbcStatementBuilder, connectionOptions));
    }

}
