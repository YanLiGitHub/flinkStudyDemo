package com.yanli.flink.config

import java.io.FileInputStream
import java.util.Properties

/**
 * @version 1.0
 * @ClassName: KafkaConfig 
 * @author YanLi
 * @date 2020/1/16 7:55 下午
 */
object KafkaConfig {
  def getKafkaConsumerConfig(): Properties ={
    val propConsumer = new Properties()
//    val inputStream = Thread.currentThread.getContextClassLoader.getResourceAsStream("kafka.properties")
//    propConsumer.load(inputStream)
//    println(propConsumer.getProperty("bootstrap.servers"))
    propConsumer.setProperty("bootstrap.servers","127.0.0.1:9092")
    propConsumer.setProperty("group.id","test-consumer-group")

    propConsumer
  }

  def getKafkaProducerConfig(): Properties ={
    val propProducer: Properties = new Properties()
    propProducer.setProperty("bootstrap.server","127.0.0.1:9092")
    propProducer
  }

}
