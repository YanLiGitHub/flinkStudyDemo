package com.yanli.flink.scala.utils

import java.util

import com.alibaba.fastjson.{JSON, JSONArray}
import com.alibaba.fastjson.serializer.{SerializeConfig, SerializerFeature}
import org.slf4j.{Logger, LoggerFactory}

import scala.reflect.ClassTag

/**
 * @author yanli
 * @date 2019/12/22 16:15
 * @version 1.0
 */
object JsonUtils {
  private val logger: Logger = LoggerFactory.getLogger(JsonUtils.getClass)

  /*
     * 功能描述:
     * 〈json 转 pojo类型〉
     * @Param: [pojo, tClass]
     * @Return: T
     * @Author: yanli
     * @Date: 2020/1/13 8:02 下午
     */
  def getObject[T](pojo: String, tClass: Class[T]): T = {
    try {
      val jsonObj: T = JSON.parseObject(pojo, tClass)
      jsonObj
    } catch {
      case exception: Exception =>
        exception.printStackTrace()
        logger.error("JSON 转化 失败")
        tClass.newInstance()
    }
  }

  /*
   * 功能描述:
   * 〈pojo 类 转 json 〉
   * @Param: T 类型
   * @Return: java.lang.String
   * @Author: yanli
   * @Date: 2020/1/13 8:10 下午
   */
  def getJson[T](tResponse: T): String = {
    val jsonString: String = JSON.toJSONString(tResponse,new SerializeConfig(true))
    jsonString
  }

  /*
   * 功能描述:
   * 〈array 转 json〉
   * @Param: List
   * @Return: java.lang.String
   * @Author: yanli
   * @Date: 2020/1/13 8:12 下午
   */
  def arrayToJson[T](ts: Array[T]): String = JSON.toJSONString(ts,new SerializeConfig(true))

  /*
   * 功能描述:
   * 〈json 转 array 〉
   * @Param: [jsonStr, tClass]
   * @Return: java.util.List<T>
   * @Author: yanli
   * @Date: 2020/1/13 8:12 下午
   */
  def jsonToArray[T:ClassTag](jsonStr: String, tClass: Class[T]): Array[T] = {
    val tList: util.List[T] = JSON.parseArray(jsonStr, tClass)
    val array: Array[T] = new Array[T](tList.size())
    for (i <- 0 to(tList.size()-1)){
      array(i) = tList.get(i)
    }
    array
  }

//  def main(args: Array[String]): Unit = {
//
//    val str: String = "[{\"id\":123,\"name\":\"张三\"},{\"id\":234,\"name\":\"李四\"},{\"id\":456,\"name\":\"wangwu\"}]"
//
//    val users1: Array[User] = jsonToArray[User](str, classOf[User])
//    for (u <- users1){
//      println(u)
//    }
//    println(users1)
//
//  }
//
//  case class User(id: Int,name :String)
}
