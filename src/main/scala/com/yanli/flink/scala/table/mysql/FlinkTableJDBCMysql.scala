package com.yanli.flink.scala.table.mysql

import com.yanli.flink.java.config.MysqlConfig
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSinkBuilder

/**
 * @version 1.0
 * @ClassName: FlinkTableToMysql 
 * @author YanLi
 * @date 2020/1/21 11:22 上午
 */
object FlinkTableJDBCMysql {
  def getMysqlContent(): JDBCAppendTableSinkBuilder ={
    val properties = MysqlConfig.getMysqlConfig()
    val builder: JDBCAppendTableSinkBuilder = new JDBCAppendTableSinkBuilder()
      .setDrivername(properties.getProperty("driverClass"))
      .setDBUrl(properties.getProperty("dbUrl"))
      .setUsername(properties.getProperty("userNmae"))
      .setPassword(properties.getProperty("passWord"))
      .setBatchSize(1000)
    builder
  }

}
