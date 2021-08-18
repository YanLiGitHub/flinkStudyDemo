package com.yanli.flink.java.streamingApi.mysql;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.yanli.flink.java.pojo.hdfs.Person;
import com.yanli.flink.java.streamingApi.kafka.FlinkConnectKafka;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author YanLi
 * @version 1.0
 * @ClassName: FlinkCDCMysql
 * @date 2021/5/25 2:46 下午
 */
public class FlinkCDCMysql {
    private static final Logger logger = LoggerFactory.getLogger(FlinkCDCMysql.class);

    /**
     * 通过CDC获取mysql数据，广播出去
     * @param environment
     * @return
     */
    public static BroadcastStream getMysqlSource(StreamExecutionEnvironment environment){
        SourceFunction<Map<String, Person>> mysqlBuilder = MySQLSource.<Map<String, Person>>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .databaseList("flink")
                .tableList("flink.example")
                .username("root")
                .password("root")
                .deserializer(new MyDebeziumDeserialization())
                .build();
        DataStreamSource<Map<String, Person>> dataStreamSource = environment.addSource(mysqlBuilder);
        BroadcastStream<Map<String, Person>> broadcastStream = dataStreamSource.broadcast(new MapStateDescriptor<String, Person>("RulesBroadcastState", String.class, Person.class));

        DataStream kafkaSource = FlinkConnectKafka.getKafkaSource(environment, "kafka_topic", "bootstrap_servers");
        kafkaSource.connect(broadcastStream)
                .process(new BroadcastProcessFunction<Person, Map<String, Person>, String>() {

                    MapStateDescriptor<String, Person> mapStateDescriptor;

                    Map<String, Person> map = new HashMap<>();

                    boolean flag = false;

                    List<Person> list = new ArrayList<>();

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        // 这里需要初始化map state 描述
                        mapStateDescriptor = new MapStateDescriptor<>("broadcast-state", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(Person.class));
                    }

                    @Override
                    public void processBroadcastElement(Map<String, Person> value, Context ctx, Collector<String> out) throws Exception {
                        if (this.map != value) {
                            for (String key : value.keySet()) {
                                System.out.println("hello world--->" + key);
                                ctx.getBroadcastState(mapStateDescriptor).put(key, value.get(key));
                            }
                            flag = true;
                            this.map = value;
                        } else {
                            System.out.println("广播流无变化");
                        }
                    }

                    @Override
                    public void processElement(Person value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        ReadOnlyBroadcastState<String, Person> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                        if (flag == true) {
                            System.out.println("list里面元素--->" + list);
                            System.out.println("list else大小--->" + list.size());
                            if (list.size() != 0) {
                                for (Person p : list) {
                                    Person statePerson = broadcastState.get(p.getName());
                                    p.setAge(statePerson.getAge());
                                    out.collect(p.toString());
                                }
                                list.clear();
                            }
                            Person statePerson = broadcastState.get(value.getName());
                            value.setAge(statePerson.getAge());
                            out.collect(value.toString());
                        } else {
                            list.add(value);
                            System.out.println("list添加元素--->" + list);
                            System.out.println("list大小--->" + list.size());
                        }
                    }

                }).print().setParallelism(1);

        return broadcastStream;
    }

    public static class MyDebeziumDeserialization implements DebeziumDeserializationSchema<Map<String,Person>>{
        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<Map<String, Person>> collector) throws Exception {
            Person p = new Person();
            Struct sourceValue = (Struct) sourceRecord.value();
            Struct afterValue = (Struct) sourceValue.get("after");
            Object name = afterValue.get("name");
            System.out.println(name);
            Object age = afterValue.get("age");
            System.out.println(age);
            p.setName(name.toString());
            p.setAge(Integer.parseInt(age.toString()));
            Map<String, Person> map = new HashMap<>();
            map.put(p.getName(), p);
            collector.collect(map);
        }

        @Override
        public TypeInformation<Map<String, Person>> getProducedType() {
            return new MapTypeInfo(String.class,Person.class);
        }
    }

}
