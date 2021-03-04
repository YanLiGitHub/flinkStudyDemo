package com.yanli.flink.scala.table.mysql

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink
import org.apache.flink.table.api.scala.StreamTableEnvironment

/**
 * @version 1.0
 * @ClassName: FlinkTableConnectMysql 
 * @author YanLi
 * @date 2020/1/21 1:47 下午
 */
object FlinkTableConnectMysql {
  def getFlinkTableToMysql(tableEnv: StreamTableEnvironment): Unit ={
    val builder: JDBCAppendTableSink = FlinkTableJDBCMysql
      .getMysqlContent()
      .setBatchSize(1000)
      .setQuery("")
      .setParameterTypes(Types.STRING,Types.DOUBLE,Types.BIG_INT)
      .build()
    tableEnv.registerTableSink("mysqlSink",builder)

  }

}
