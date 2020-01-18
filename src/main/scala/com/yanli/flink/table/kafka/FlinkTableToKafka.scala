package com.yanli.flink.table.kafka

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.Kafka

/**
 * @version 1.0
 * @ClassName: FlinkTableToKafka
 * @author YanLi
 * @date 2020/1/17 2:05 下午
 */
object FlinkTableToKafka {
  def getKafkaConnect(): Unit ={
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val envSettions: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, envSettions)
    tableEnv.connect(
      new Kafka()
        .version("universal")
        .topic("topic")
        .property("bootstrap.server","127.0.0.1:9092")
        .property("group.id","test-consumer-group")
        .startFromLatest()
    )
    env.execute("flinkTable kafka")
  }

  def main(args: Array[String]): Unit = {
    getKafkaConnect()
  }
}
