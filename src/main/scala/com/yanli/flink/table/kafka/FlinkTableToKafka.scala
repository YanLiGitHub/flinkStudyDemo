package com.yanli.flink.table.kafka


import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table, Types}
import org.apache.flink.table.api.scala.StreamTableEnvironment
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
      .field("",Types.STRING())
      .field("",Types.SQL_TIMESTAMP())

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, envSettions)
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
          .deriveSchema()
        )
        .inAppendMode()
        .registerTableSource("kafkaSource")

    val kafkaTable: Table = tableEnv.sqlQuery("select * from kafkaSource")
    kafkaTable
  }

}
