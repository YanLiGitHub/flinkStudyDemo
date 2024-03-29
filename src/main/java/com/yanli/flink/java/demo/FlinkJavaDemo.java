package com.yanli.flink.java.demo;

import com.alibaba.fastjson.JSONObject;
import com.yanli.flink.java.config.HBaseConfig;
import com.yanli.flink.java.pojo.kafka.BehaviorEvent;
import com.yanli.flink.java.pojo.mysql.TulingLectureLabel;
import com.yanli.flink.java.streamingApi.elasticsearch.FlinkConnectElasticSearch;
import com.yanli.flink.java.streamingApi.kafka.FlinkConnectKafka;
import com.yanli.flink.java.streamingApi.mysql.SampleAsyncDatabase;
import com.yanli.flink.java.utils.JavaJsonUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.Collector;
import org.codehaus.janino.Java;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author yanli
 * @version 1.0
 * @date 2019/12/22 16:12
 */
public class FlinkJavaDemo {
    private static final Logger logger = LoggerFactory.getLogger(FlinkJavaDemo.class);


    public static void main(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        String kafkaTopic = parameterTool.get("kafka_topic");
        String bootstrapServers = parameterTool.get("bootstrap_servers");
        String zookeeperQuorum = parameterTool.get("zookeeper_quorum");
        int parallelism = parameterTool.getInt("parallelism");
        int HBaseWriterBufferSize = parameterTool.getInt("HBaseWriterBufferSize");
        long startTimestamp = parameterTool.getLong("startTimestamp");


        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(parallelism);
        //设置state存储路径
        environment.setStateBackend(new FsStateBackend("hdfs:///checkpoints-data/"));
        environment.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        DataStream<String> kafkaSource = FlinkConnectKafka.getKafkaSource(environment, kafkaTopic, bootstrapServers);

        SingleOutputStreamOperator<Map<String, String>> streamOperator = kafkaSource.flatMap(new FlatMapFunction<String, Map<String, String>>() {
            @Override
            public void flatMap(String value, Collector<Map<String, String>> collector) throws Exception {
                logger.info("kafka原始数据 ====>" + value);
                BehaviorEvent behaviorEvent = JavaJsonUtil.getObject(value, BehaviorEvent.class);

                JSONObject hbaseWriterJson = new JSONObject();
                if (behaviorEvent != null) {


                    hbaseWriterJson.put("eventId", behaviorEvent.getEventId());
                    hbaseWriterJson.put("appId", behaviorEvent.getAppId());
                    hbaseWriterJson.put("orgCode", behaviorEvent.getOrgCode());
                    hbaseWriterJson.put("requestId", behaviorEvent.getRequestId());
                    hbaseWriterJson.put("sourceRequestType", behaviorEvent.getSourceRequestType());
                    hbaseWriterJson.put("gradeCode", String.valueOf(behaviorEvent.getGradeCode()));
                    hbaseWriterJson.put("subjectCode", String.valueOf(behaviorEvent.getSubjectCode()));
                    hbaseWriterJson.put("termCode", String.valueOf(behaviorEvent.getTermCode()));
                    hbaseWriterJson.put("userId", behaviorEvent.getUserId());
                    hbaseWriterJson.put("identityCode", behaviorEvent.getIdentityCode());
                    hbaseWriterJson.put("subTime", String.valueOf(behaviorEvent.getSubTime()));
                    hbaseWriterJson.put("behaviorCode", String.valueOf(behaviorEvent.getBehaviorParam().getBehaviorCode()));
                    hbaseWriterJson.put("elementTypeCode", String.valueOf(behaviorEvent.getBehaviorParam().getBehaviorContent().getElementTypeCode()));
                    hbaseWriterJson.put("elementId", behaviorEvent.getBehaviorParam().getBehaviorContent().getElementId());

                    List<BehaviorEvent.AnswerBehavior> queAnswerList = behaviorEvent.getBehaviorParam().getBehaviorContent().getQueAnswerList();
                    //预留params 字段
                    hbaseWriterJson.put("params", behaviorEvent.getParams() == null ? new JSONObject().toString() : behaviorEvent.getParams().toString());

                    for (BehaviorEvent.AnswerBehavior answerBehavior : queAnswerList) {
                        hbaseWriterJson.put("answerId", answerBehavior.getAnswerId());
                        hbaseWriterJson.put("queId", answerBehavior.getQueId());
                        hbaseWriterJson.put("queSort", String.valueOf(answerBehavior.getQueSort()));
                        hbaseWriterJson.put("stuId", answerBehavior.getStuId());
                        hbaseWriterJson.put("stuName", answerBehavior.getStuName());
                        hbaseWriterJson.put("ansRes", String.valueOf(answerBehavior.getAnsRes()));
                        hbaseWriterJson.put("ansDur", String.valueOf(answerBehavior.getAnsDur()));
                        hbaseWriterJson.put("queScore", String.valueOf(answerBehavior.getQueScore()));
                        hbaseWriterJson.put("stuScore", String.valueOf(answerBehavior.getStuScore()));
                        hbaseWriterJson.put("operateType", answerBehavior.getOperateType());
                        hbaseWriterJson.put("answer", JavaJsonUtil.listToJson(answerBehavior.getAnswer()));

                        ArrayList<String> colNameList = new ArrayList<>(hbaseWriterJson.keySet());
                        hbaseWriterJson.put(HBaseConfig.ROW_KEY, behaviorEvent.getUserId() + "_" + behaviorEvent.getEventId() + "_" + hbaseWriterJson.get("answerId"));
                        hbaseWriterJson.put(HBaseConfig.COLUMN_NAME_LIST, JavaJsonUtil.listToJson(colNameList));

                        logger.info("hbaseWriterJson ====>" + hbaseWriterJson.toJSONString());

                        collector.collect(JavaJsonUtil.getObject(hbaseWriterJson.toJSONString(), Map.class));

                    }
                }
            }
        });
        SingleOutputStreamOperator<TulingLectureLabel> tulingLectureLabelSingleOutputStreamOperator = kafkaSource.map(str -> JavaJsonUtil.getObject(str, TulingLectureLabel.class));
        AsyncDataStream.unorderedWait(tulingLectureLabelSingleOutputStreamOperator,new SampleAsyncDatabase(),1000L,TimeUnit.MILLISECONDS,100);

        streamOperator.addSink(FlinkConnectElasticSearch.addSink(new ArrayList<>(),1));

        streamOperator.keyBy(new KeySelector<Map<String, String>, String>() {
            @Override
            public String getKey(Map<String, String> value) throws Exception {
                for (String mapKey : value.keySet()){
                    return mapKey;
                }
                return null;
            }
        }).process(new KeyedProcessFunction<String, Map<String, String>, Integer>() {
            private ValueState<Integer> sumState;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor("sum", Integer.TYPE);
                sumState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public void processElement(Map<String, String> value, Context ctx, Collector<Integer> out) throws Exception {
                Integer oldSum = sumState.value();
                int newSum = oldSum == null ? 0 : oldSum;
                newSum += 1;
                sumState.update(newSum);
                out.collect(newSum);
            }
        }).print();
    }
}
