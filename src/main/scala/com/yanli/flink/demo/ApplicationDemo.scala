package com.yanli.flink.demo

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author yanli
 * @date 2019/12/22 15:23
 * @version 1.0
 */
object ApplicationDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.execute("Flink Test")
  }

}
