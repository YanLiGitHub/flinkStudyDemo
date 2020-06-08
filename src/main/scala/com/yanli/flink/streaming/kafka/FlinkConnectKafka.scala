package com.yanli.flink.streaming.kafka



import java.lang
import java.nio.charset.StandardCharsets

import com.yanli.flink.config.KafkaConfig
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.flink.api.scala._
import org.apache.kafka.clients.producer.ProducerRecord


/**
 * @author yanli
 * @date 2019/12/22 16:13
 * @version 1.0
 */
object FlinkConnectKafka {
  def getKafkaSource(env: StreamExecutionEnvironment,topic: String): DataStream[String] ={
    val flinkConsumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), KafkaConfig.getKafkaConsumerConfig())
    flinkConsumer.setStartFromLatest()    //设置开始消费的位置为latest

    val data: DataStream[String] = env.addSource(flinkConsumer)

    data
  }

  def getKafkaSink(dataStream :DataStream[String],topic :String): Unit ={
    val flinkProducer: FlinkKafkaProducer[String] = new FlinkKafkaProducer[String](
      topic,
      //新的api需要用KafkaSerializationSchema ,跟SimpleStringSchema 同样的实现
      new KafkaSerializationSchema[String] {
        override def serialize(element: String, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = new ProducerRecord[Array[Byte], Array[Byte]](topic,element.getBytes(StandardCharsets.UTF_8))
      },
      KafkaConfig.getKafkaProducerConfig(),
      FlinkKafkaProducer.Semantic.EXACTLY_ONCE
    )

    flinkProducer.setWriteTimestampToKafka(true)

    dataStream.addSink(flinkProducer)
  }

}
