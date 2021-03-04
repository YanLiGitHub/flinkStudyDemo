package com.yanli.flink.scala.streaming.mysql

import java.sql.Types

import org.apache.flink.api.java.io.jdbc.{JDBCInputFormat, JDBCOutputFormat}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.types.Row


/**
 * @version 1.0
 * @ClassName: FlinkConnectMysql 
 * @author YanLi
 * @date 2020/1/20 4:29 下午
 */
object FlinkConnectMysql {
  /*
   * 功能描述:
   * 〈查询mysql 表〉
   * @Param: [env, tableName, rowTypeInfo]
   * @Return: org.apache.flink.streaming.api.scala.DataStream<org.apache.flink.types.Row>
   * @Author: yanli
   * @Date: 2020/1/20 4:37 下午
   */
  def selectAll(env:StreamExecutionEnvironment,tableName: String,rowTypeInfo: RowTypeInfo): DataStream[Row] ={
    val inputFormatBuilder = FlinkJDBCFormat.getJDBCInputFormat()
    val inputFormat: JDBCInputFormat = inputFormatBuilder.setQuery("SELECT * FROM " + tableName)
      .setRowTypeInfo(rowTypeInfo)
      .finish()
    val rowData: DataStream[Row] = env.createInput(inputFormat)
    rowData
  }

  /*
   * 功能描述:
   * 〈向 表里插入数据〉
   * @Param: [tableName, rows]
   * @Return: void
   * @Author: yanli
   * @Date: 2020/1/20 4:47 下午
   */
  def insertTable(tableName: String,rows:Array[Row]): Unit ={
    val outputFormatBuilder: JDBCOutputFormat.JDBCOutputFormatBuilder = FlinkJDBCFormat.getJDBCOutputFormat()
    val outputFormat: JDBCOutputFormat = outputFormatBuilder.setQuery("insert into " + tableName + "values(?,?,?)")
      .setSqlTypes(Array[Int](Types.INTEGER, Types.VARCHAR, Types.DOUBLE))
      .finish()

    outputFormat.open(1,1)

    for (row <- rows){
      outputFormat.writeRecord(row)
    }

    outputFormat.close()
  }

  /*
   * 功能描述:
   * 〈更新表中的一条数据〉
   * @Param: [tableName, row]
   * @Return: void
   * @Author: yanli
   * @Date: 2020/1/20 4:51 下午
   */
  def updataTable(tableName: String,row: Row): Unit ={
    val outputFormatBuilder: JDBCOutputFormat.JDBCOutputFormatBuilder = FlinkJDBCFormat.getJDBCOutputFormat()
    val outputFormat: JDBCOutputFormat = outputFormatBuilder.setQuery("update "+tableName+" set name = ?, password = ? where id = ?")
      .setSqlTypes(Array[Int](Types.INTEGER, Types.VARCHAR, Types.DOUBLE))
      .finish()
    outputFormat.open(1,1)

    outputFormat.writeRecord(row)

    outputFormat.close()
  }

}
