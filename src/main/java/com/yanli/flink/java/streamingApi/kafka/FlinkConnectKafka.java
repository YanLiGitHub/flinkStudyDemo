package com.yanli.flink.java.streamingApi.kafka;

import com.yanli.flink.java.config.KafkaConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

/**
 * @author YanLi
 * @version 1.0
 * @ClassName: FlinkConnectKafka
 * @date 2021/2/24 5:11 下午
 */
public class FlinkConnectKafka {
    /**
     *  创建kafka source
     * @param environment
     * @param topic
     * @return
     */
    public static DataStream getKafkaSource(StreamExecutionEnvironment environment, String topic,String bootstrap_servers) {
        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), KafkaConfig.getKafkaConsumerConfig(bootstrap_servers));
        flinkKafkaConsumer.setStartFromLatest();
        flinkKafkaConsumer.setCommitOffsetsOnCheckpoints(true);

        return environment.addSource(flinkKafkaConsumer);
    }

    /**
     * 创建kafka sink
     * @param dataStream
     * @param topic
     * @param topicKey
     */
    public static void getKafkaSink(DataStream dataStream, String topic, String topicKey) {
        FlinkKafkaProducer<String> flinkKafkaProducer = new FlinkKafkaProducer<String>(topic, new KafkaSerializationSchema<String>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                return new ProducerRecord(topic, topicKey.getBytes(StandardCharsets.UTF_8), element.getBytes(StandardCharsets.UTF_8));
            }
        }, KafkaConfig.getKafkaProducerConfig()
                , FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        flinkKafkaProducer.setWriteTimestampToKafka(true);

        dataStream.addSink(flinkKafkaProducer);
    }
}
