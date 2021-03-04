package com.yanli.flink.java.streamingApi.mysql;

import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat.JDBCInputFormatBuilder;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat.JDBCOutputFormatBuilder;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Types;
import java.util.List;

/**
 * @author YanLi
 * @version 1.0
 * @ClassName: FlinkConnectMysql
 * @date 2021/2/24 5:12 下午
 */
public class FlinkConnectMysql {
    private static final Logger logger = LoggerFactory.getLogger(FlinkConnectMysql.class);

    /**
     *  查询mysql 数据
     * @param env
     * @param tableName
     * @param rowTypeInfo
     * @return
     */
    public static DataStream<Row> selectAll(StreamExecutionEnvironment env, String tableName, RowTypeInfo rowTypeInfo){
        JDBCInputFormatBuilder inputFormatBuilder = FlinkJDBCFormat.getJDBCInputFormat();
        JDBCInputFormat inputFormat = inputFormatBuilder.setQuery("SELECT * FROM " + tableName)
                .setRowTypeInfo(rowTypeInfo)
                .finish();
        DataStream<Row> dataStream = env.createInput(inputFormat);

        return dataStream;
    }

    /**
     * 功能描述:
     * 〈向 表里插入数据〉
     * @Param: [tableName, rows]
     * @Return: void
     * @Author: yanli
     * @Date: 2021/2/22 11:32 上午
     */
    public static void insertTable(String tableName, List<Row> rowList){
        JDBCOutputFormatBuilder outputFormatBuilder = FlinkJDBCFormat.getJDBCOutputFormat();
        JDBCOutputFormat outputFormat = outputFormatBuilder.setQuery("insert into " + tableName + "values(?,?,?)")
                .setSqlTypes(new int[]{Types.INTEGER, Types.VARCHAR, Types.DOUBLE})
                .finish();
        try {
            outputFormat.open(1,1);
            for(Row row:rowList){
                outputFormat.writeRecord(row);
            }
            outputFormat.close();
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Close HBase Exception:", e.toString());
        }

    }

    /**
     * 功能描述:
     * 〈更新表中的一条数据〉
     * @Param: [tableName, row]
     * @Return: void
     * @Author: yanli
     * @Date: 2021/2/22 11:32 上午
     */
    public static void updataTable(String tableName, Row row){
        JDBCOutputFormatBuilder outputFormatBuilder = FlinkJDBCFormat.getJDBCOutputFormat();
        JDBCOutputFormat outputFormat = outputFormatBuilder.setQuery("update " + tableName + " set name = ?, password = ? where id = ?")
                .setSqlTypes(new int[]{Types.INTEGER, Types.VARCHAR, Types.DOUBLE})
                .finish();
        try {
            outputFormat.open(1,1);

            outputFormat.writeRecord(row);

            outputFormat.close();
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Close HBase Exception:", e.toString());
        }
    }
}
