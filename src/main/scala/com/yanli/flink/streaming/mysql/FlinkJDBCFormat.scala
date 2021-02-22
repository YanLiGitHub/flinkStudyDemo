package com.yanli.flink.streaming.mysql

import com.yanli.flink.config.MysqlConfig
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat.JDBCInputFormatBuilder
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat.JDBCOutputFormatBuilder
import org.apache.flink.api.java.io.jdbc.{JDBCInputFormat, JDBCOutputFormat}


/**
 * @version 1.0
 * @ClassName: FlinkConnectMysql 
 * @author YanLi
 * @date 2020/1/20 3:23 下午
 */
object FlinkJDBCFormat {

  private val properties = MysqlConfig.getMysqlConfig()
  /*
   * 功能描述:
   * 〈创建JDBCInputFormatBuilder〉
   * @Param: []
   * @Return: org.apache.flink.api.java.io.jdbc.JDBCInputFormat.JDBCInputFormatBuilder
   * @Author: yanli
   * @Date: 2020/1/20 4:35 下午
   */
  def getJDBCInputFormat(): JDBCInputFormatBuilder ={
    val inputFormatBuilder: JDBCInputFormatBuilder = JDBCInputFormat.buildJDBCInputFormat()
      .setDrivername(properties.getProperty("driverClass"))
      .setDBUrl(properties.getProperty("dbUrl"))
      .setUsername(properties.getProperty("userNmae"))
      .setPassword(properties.getProperty("passWord"))

    inputFormatBuilder
  }
  /*
   * 功能描述:
   * 〈创建JDBCOutputFormatBuilder〉
   * @Param: []
   * @Return: org.apache.flink.api.java.io.jdbc.JDBCOutputFormat.JDBCOutputFormatBuilder
   * @Author: yanli
   * @Date: 2020/1/20 4:36 下午
   */
  def getJDBCOutputFormat(): JDBCOutputFormatBuilder ={
    val outputFormatBuilder: JDBCOutputFormatBuilder = JDBCOutputFormat.buildJDBCOutputFormat()
      .setDrivername(properties.getProperty("driverClass"))
      .setDBUrl(properties.getProperty("dbUrl"))
      .setUsername(properties.getProperty("userNmae"))
      .setPassword(properties.getProperty("passWord"))
    outputFormatBuilder
  }
}
