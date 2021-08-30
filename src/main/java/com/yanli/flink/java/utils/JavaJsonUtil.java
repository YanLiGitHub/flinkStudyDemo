package com.yanli.flink.java.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @version 1.0
 * @ClassName: JsonUtil
 * @author YanLi
 * @date 2020/1/15 1:35 下午
 */
public class JavaJsonUtil {
    private static final Logger logger = LoggerFactory.getLogger(JavaJsonUtil.class);
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

    /**
     * @Description: json转换字段结构，驼峰转下划线、下划线转驼峰等等
     * @Param: jsonStr 要转换的json字符串
     * @Param: sourceType 字段串中字段的数据格式
     * @Param: distType 将要转换成的字段格式
     * @return: java.lang.String
     * @Create: 2021-08-10 13:24:16
     */
    public static String jsonTransferByGuava(String jsonStr, CaseFormat sourceType, CaseFormat distType) {

        Map<String, String> map = JSON.parseObject(jsonStr, Map.class);
        Map<String, Object> newMap = new HashMap<>();
        Set<Map.Entry<String, String>> entries = map.entrySet();
        for (Map.Entry<String, String> entry : entries) {
            // key的转换
            String newKey = sourceType.to(distType, entry.getKey());
            // 将新key放进结果map中
            newMap.put(newKey, entry.getValue());
        }
        return JSON.toJSONString(newMap);
    }
}

