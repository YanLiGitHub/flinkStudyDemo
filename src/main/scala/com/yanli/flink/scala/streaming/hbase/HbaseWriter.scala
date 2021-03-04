package com.yanli.flink.scala.streaming.hbase

import com.yanli.flink.scala.config.HBaseConfig
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{BufferedMutator, BufferedMutatorParams, Connection, ConnectionFactory, Put, Scan}
import org.apache.hadoop.hbase.util.Bytes

/**
 * @version 1.0
 * @ClassName: HbaseWriter 
 * @author YanLi
 * @date 2020/6/8 2:19 下午
 *  RichSinkFunction[String] 类型由dataStream 的数据类型决定
 */
class HbaseWriter extends RichSinkFunction[String] {
  private var conn: Connection = null
  private var mutator: BufferedMutator = null
  private var scan: Scan = null
  //缓存计数器
  private var count: Int = 0
  private var tableName: String = null
  private var cf: String = null
  private var columnName: String = null

  def this(tableName: String,cf: String,columnName: String){
    this()
    this.tableName = tableName
    this.cf= cf
    this.columnName = columnName
  }

  /**
   * 功能描述:
   * 〈获取连接，缓存〉
   * @Param: [parameters]
   * @Return: void
   * @Author: yanli
   * @Date: 2020/6/8 7:15 下午
   */
  override def open(parameters: Configuration): Unit = {
    conn = ConnectionFactory.createConnection(HBaseConfig.getHBaseConfig())

    val params: BufferedMutatorParams = new BufferedMutatorParams(TableName.valueOf(tableName))
    params.writeBufferSize(1024 * 1024) //设置缓存大小（byte）
    mutator = conn.getBufferedMutator(params)
    count = 0
  }

  override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
    val array: Array[String] = value.split(",")
    //设置写的rowkey
    val put: Put = new Put(Bytes.toBytes(array(0)))
    //设置列簇、列名、值
    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(columnName), Bytes.toBytes(array(1)))
    mutator.mutate(put)
    //满2000条写一次hbase
    if (count >= 2000) {
      mutator.flush()
      count = 0
    }
    count += 1

  }

  override def close(): Unit = {
    try {
      if (conn != null) {
        conn.close()
      }
    } catch {
      case exception: Exception =>println(exception)
    }
  }
}
