package com.yanli.flink.streaming.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @version 1.0
 * @ClassName: JsonUtil
 * @author YanLi
 * @date 2020/1/15 1:35 下午
 */
public class JsonUtil {
    private static final Logger logger = LoggerFactory.getLogger(JsonUtil.class);
    /*
     * 功能描述:
     * 〈json 转 pojo类型〉
     * @Param: [pojo, tClass]
     * @Return: T
     * @Author: yanli
     * @Date: 2020/1/13 8:02 下午
     */
    public static <T> T getObject(String pojo,Class<T> tClass){
        try {
            JSONObject.parseObject(pojo,tClass);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(tClass + "转 JSON 失败");
        }
        return null;
    }

    /*
     * 功能描述:
     * 〈pojo 类 转 json 〉
     * @Param: T 类型
     * @Return: java.lang.String
     * @Author: yanli
     * @Date: 2020/1/13 8:10 下午
     */
    public static <T> String getJson(T tResponse){
        String jsonString = JSONObject.toJSONString(tResponse);
        return jsonString;
    }

    /*
     * 功能描述:
     * 〈list 转 json〉
     * @Param: List
     * @Return: java.lang.String
     * @Author: yanli
     * @Date: 2020/1/13 8:12 下午
     */
    public static <T> String listToJson(List<T> ts){
        return JSON.toJSONString(ts);
    }

    /*
     * 功能描述:
     * 〈json 转 list 〉
     * @Param: [jsonStr, tClass]
     * @Return: java.util.List<T>
     * @Author: yanli
     * @Date: 2020/1/13 8:12 下午
     */
    public static <T> List<T> jsonToList(String jsonStr, Class<T> tClass) {
        return JSONArray.parseArray(jsonStr, tClass);
    }


}

