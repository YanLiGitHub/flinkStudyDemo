package com.yanli.flink.java.streamingApi.mysql;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.yanli.flink.java.config.MysqlConfig;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author YanLi
 * @version 1.0
 * @ClassName: MysqlCDCSource
 * @date 2021/7/13 10:37 上午
 */
public class MysqlCDCSource {
    private static final Logger logger = LoggerFactory.getLogger(MysqlCDCSource.class);


    public static DataStream<String> getMysqlSource(StreamExecutionEnvironment environment) {

//        Properties debeziumProperties = new Properties();
//        不获取所有表的ddl,权限小时可以使用该方案
//        debeziumProperties.setProperty("database.history.store.only.monitored.tables.ddl",String.valueOf(true));
//        debeziumProperties.setProperty("database.history.store.only.captured.tables.ddl",String.valueOf(true));

        SourceFunction<String> mysqlBuilder = MySQLSource.<String>builder()
                .hostname(MysqlConfig.CDC_HOST)
                .port(MysqlConfig.CDC_PORT)
                .databaseList(MysqlConfig.CDC_DATABASE_NAME)
                .tableList(MysqlConfig.CDC_TABLE_NAME)
                .username(MysqlConfig.CDC_USER_NAME)
                .password(MysqlConfig.CDC_PASSWORD)
                //配置debezium 连接mysql参数
//                .debeziumProperties(debeziumProperties)
                .deserializer(new JSONObjectDebeziumDeserialization())
                .build();
        return environment.addSource(mysqlBuilder);
    }

    private static class JSONObjectDebeziumDeserialization implements DebeziumDeserializationSchema<String> {
        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<String> collector) {
            logger.info("sourceRecord:" + sourceRecord.toString());
            Envelope.Operation op = Envelope.operationFor(sourceRecord);
            JSONObject returnJson = new JSONObject();
            Struct sourceValue = (Struct) sourceRecord.value();
            Struct afterValue = (Struct) sourceValue.get("after");
            Struct beforeValue = (Struct) sourceValue.get("before");
            if (op == Envelope.Operation.DELETE) {
                returnJson.put("isDelete", 1);
                for (Field field : beforeValue.schema().fields()) {
                    returnJson.put(field.name(), beforeValue.get(field));
                }
                collector.collect(returnJson.toJSONString());
            } else {
                returnJson.put("isDelete", 0);
                for (Field field : afterValue.schema().fields()) {
                    returnJson.put(field.name(), afterValue.get(field));
                }
                collector.collect(returnJson.toJSONString());
            }
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }
}
