package com.yanli.flink.streaming.hbase

import java.util

import com.yanli.flink.config.HBaseConfig
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.hadoop.hbase.{Cell, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Result, ResultScanner, Scan, Table}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConverters._

/**
 * @version 1.0
 * @ClassName: HbaseReader 
 * @author YanLi
 * @date 2020/6/8 2:18 下午
 */
class HbaseReader extends RichSourceFunction[(String,String)]{
  private var conn:Connection = null
  private var table:Table = null
  private var scan:Scan = null
  private var tableName:String = null
  private var cf:String = null
  private var startRow:String = null
  private var endRow:String = null

  def this(tableName:String,cf:String){
    this(tableName,cf,null,null)
  }

  def this(tableName:String,cf:String,startRow:String,endRow:String){
    this()
    this.tableName = tableName
    this.cf = cf
    this.startRow = startRow
    this.endRow = endRow
  }
  /**
   * 功能描述:
   * 〈连接hbase客户端〉
   *
   * @Param: 客户端连接配置
   * @Return: void
   * @Author: yanli
   * @Date: 2020/6/8 2:24 下午
   */
  override def open(parameters: Configuration): Unit = {
//    val testTable = TableName.valueOf("test")
//    val cf:String = "cf1"
    conn = ConnectionFactory.createConnection(HBaseConfig.getHBaseConfig())
    val table1: Table = conn.getTable(TableName.valueOf(tableName))
    val scan1: Scan = new Scan()
    scan.withStartRow(Bytes.toBytes(startRow))
    scan.withStopRow(Bytes.toBytes(endRow))
    scan.addFamily(Bytes.toBytes(cf))

  }

  /**
   * 功能描述:
   * 〈通过scan方法扫描hbase表，返回查询到的结果〉
   * @Param: [sourceContext]
   * @Return: void
   * @Author: yanli
   * @Date: 2020/6/8 3:40 下午
   */
  override def run(sourceContext: SourceFunction.SourceContext[(String, String)]): Unit = {
    val scanner: ResultScanner = table.getScanner(scan)
    val iterator: util.Iterator[Result] = scanner.iterator()
    while (iterator.hasNext){
      val result = iterator.next()
      val rowKey: String = Bytes.toString(result.getRow)
      val buffer = new StringBuffer()
      for (cell:Cell <- result.listCells().asScala){
        val value: String = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
        buffer.append(value).append("_")
      }
      //去除最后一个下划线分隔符
      val resultValue = buffer.replace(buffer.length() - 1, buffer.length(), "").toString
      sourceContext.collect((rowKey,resultValue))
    }
  }

  override def cancel(): Unit = {}

  override def close(): Unit = {
    try {
      if (table != null) {
        table.close()
      }
      if (conn != null) {
        conn.close()
      }
    } catch {
      case exception: Exception => println(exception.getMessage)
    }
  }
}
