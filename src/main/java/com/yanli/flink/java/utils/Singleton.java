package com.yanli.flink.java.utils;

import com.alibaba.fastjson.serializer.SerializeConfig;

/**
 * @author YanLi
 * @version 1.0
 * @ClassName: Singleton
 * @date 2021/7/14 4:49 下午
 */
public class Singleton {
    private static class SingletonSerializeConfig {
        private static final SerializeConfig CONFIG = new SerializeConfig();
    }
    public static final SerializeConfig getInstance() {
        return SingletonSerializeConfig.CONFIG;
    }
}
