package com.yanli.flink.demo

import com.yanli.flink.streaming.kafka.FlinkConnectKafka
import com.yanli.flink.config.KafkaConfig
import com.yanli.flink.streaming.hbase.{HbaseReader, HbaseWriter}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

/**
 * @author yanli
 * @date 2019/12/22 15:23
 * @version 1.0
 */
object ApplicationDemo {
  def main(args: Array[String]): Unit = {
    //获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    val data: DataStream[String] = FlinkConnectKafka.getKafkaSource(env, "flink-kafka-topic")
    val result: DataStream[(String,Int)] = data.map(x => (x, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)
    result.print()
    val hbaseSource: DataStream[(String, String)] = env.addSource(new HbaseReader("test","cf1"))
    val hbaseSink: DataStreamSink[String] = data.addSink(new HbaseWriter(tableName = "test", cf = "cf2", columnName = "name"))

    env.execute("Flink Test")
  }

}
