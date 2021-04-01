package com.yanli.flink.scala.table.kafka

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table, Types}
import org.apache.flink.table.descriptors.{Json, Kafka, Schema}


/**
 * @version 1.0
 * @ClassName: FlinkTableToKafka
 * @author YanLi
 * @date 2020/1/17 2:05 下午
 */
object FlinkTableToKafka {
  def getKafkaConnect(env: StreamExecutionEnvironment): Table ={
    val envSettions: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val schema = new Schema()
      .field("",DataTypes.STRING())
      .field("",DataTypes.TIMESTAMP())

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, envSettions)
    //创建临时表
    tableEnv.executeSql("CREATE [TEMPORARY] TABLE MyTable (...) WITH (...)")
    tableEnv.connect(
      new Kafka()
        .version("universal")
        .topic("topic")
        .property("bootstrap.server","127.0.0.1:9092")
        .property("group.id","test-consumer-group")
        .startFromLatest()
    ).withSchema(schema)
        .withFormat(new Json()
          .failOnMissingField(true)
        )
        .inAppendMode()
        .createTemporaryTable("kafkaSource")

    val kafkaTable: Table = tableEnv.sqlQuery("select * from kafkaSource")
    kafkaTable
  }

}
