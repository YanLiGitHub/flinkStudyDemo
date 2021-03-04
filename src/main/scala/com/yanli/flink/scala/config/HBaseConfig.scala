package com.yanli.flink.scala.config

import java.util.Properties

import org.apache.hadoop.conf
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}

/**
 * @version 1.0
 * @ClassName: HBaseConfig 
 * @author YanLi
 * @date 2020/6/8 3:50 下午
 */
object HBaseConfig {
  def getHBaseConfig(): Configuration = {
    val properties = new Properties()
    val inputStream = Thread.currentThread.getContextClassLoader.getResourceAsStream("hbase.properties")
    properties.load(inputStream)

    val configuration: conf.Configuration = HBaseConfiguration.create()
    configuration.set(HConstants.ZOOKEEPER_QUORUM, properties.getProperty("hbase.zookeeper.quorum"))
    configuration.set(HConstants.ZOOKEEPER_CLIENT_PORT, properties.getProperty("hbase.zookeeper.property.clientPort"))
    configuration.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, properties.getProperty("hbase.client.operation.timeout").toInt)
    configuration.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, properties.getProperty("hbase.client.scanner.timeout.period").toInt)
    configuration
  }
}
