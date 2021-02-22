package com.yanli.flink.config

import java.util.Properties

/**
 * @version 1.0
 * @ClassName: MysqlConfig 
 * @author YanLi
 * @date 2020/1/20 3:24 下午
 */
object MysqlConfig {
  def getMysqlConfig(): Properties ={
    val properties = new Properties()
    val inputStream = Thread.currentThread.getContextClassLoader.getResourceAsStream("mysql.properties")
    properties.load(inputStream)
//    println(properties.getProperty("driverClass"))
    properties
  }

}
