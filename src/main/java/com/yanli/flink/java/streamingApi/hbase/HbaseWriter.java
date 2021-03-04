package com.yanli.flink.java.streamingApi.hbase;


import com.yanli.flink.java.config.HBaseConfig;
import com.yanli.flink.java.utils.JavaJsonUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static com.yanli.flink.java.config.HBaseConfig.COLUMN_NAME_LIST;
import static com.yanli.flink.java.config.HBaseConfig.ROW_KEY;


/**
 * @author YanLi
 * @version 1.0
 * @ClassName: HbaseWriter
 * @date 2021/2/24 5:10 下午
 * RichSinkFunction[String] 类型由dataStream 的数据类型决定
 */
public class HbaseWriter extends RichSinkFunction<Map<String, String>> {
    private static final Logger logger = LoggerFactory.getLogger(HbaseWriter.class);

    private Connection conn;
    private BufferedMutator mutator;
    private Scan scan;

    //计数器，控制数据刷入hbase
    private int count;

    private String tableName;
    private String cf;
    private String zookeeperQuorum;

    public HbaseWriter(String tableName, String cf,String zookeeperQuorum) {
        this.tableName = tableName;
        this.cf = cf;
        this.zookeeperQuorum = zookeeperQuorum;
    }

    /**
     * 功能描述:
     * 〈获取连接，缓存〉
     *
     * @Param: [parameters]
     * @Return: void
     * @Author: yanli
     * @Date: 2021/2/24 5:10 下午
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        conn = ConnectionFactory.createConnection(HBaseConfig.getHBaseConfig(zookeeperQuorum));

        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tableName));
        params.writeBufferSize(10 * 1024 * 1024); //设置缓存大小（byte）
        mutator = conn.getBufferedMutator(params);
        count = 0;
    }

    @Override
    public void close() throws Exception {
        try {
            mutator.flush();
            conn.close();
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Close HBase Exception:", e.toString());
        }
    }


    @Override
    public void invoke(Map<String, String> value, Context context) throws Exception {
        String rowkey = value.get(ROW_KEY);
        List<String> columnNameList = JavaJsonUtil.jsonToList(value.get(COLUMN_NAME_LIST), String.class);
        //设置写的rowkey
        Put put = new Put(Bytes.toBytes(rowkey));
        //设置列簇、列名、值
        for (String column: columnNameList) {
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(column), value.get(column).getBytes(StandardCharsets.UTF_8));
        }

        mutator.mutate(put);
        mutator.flush();
        //满200条写一次hbase
//        if (count >= 200) {
//            mutator.flush();
//            count = 0;
//        }
//        count += 1;
    }
}
