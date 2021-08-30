package com.yanli.flink.java.streamingApi.redis;

import com.alibaba.fastjson.JSONObject;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.redis.client.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;

/**
 * @author YanLi
 * @version 1.0
 * @ClassName: SampleAsyncRedis
 * @date 2021/8/18 5:29 下午
 * 通过异步IO读取外部数据库数据
 */
public class SampleAsyncRedis extends RichAsyncFunction<JSONObject, JSONObject> {

    private static final Logger logger = LoggerFactory.getLogger(SampleAsyncRedis.class);

    private transient RedisAPI redisAPI;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        RedisOptions config = new RedisOptions();
        config.setEndpoints(Arrays.asList("127.0.0.1:6379"));

        VertxOptions vo = new VertxOptions();
        //使用的Event Loop线程的数量
        vo.setEventLoopPoolSize(10);
        //Worker线程的最大数量
        vo.setWorkerPoolSize(20);

        Vertx vertx = Vertx.vertx(vo);

        Redis client = Redis.createClient(vertx, config);
        redisAPI = RedisAPI.api(client);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (redisAPI != null)
            redisAPI.close();
    }

    @Override
    public void asyncInvoke(final JSONObject input, final ResultFuture<JSONObject> resultFuture) {
        String fruit = input.getString("fruit");
        // 获取hash-key值
//            redisClient.hget(fruit,"hash-key",getRes->{
//            });
        // 直接通过key获取值，可以类比
        redisAPI.get(fruit, getRes -> {
            if (getRes.succeeded()) {
                Response result = getRes.result();
                if (result == null) {
                    resultFuture.complete(null);
                    return;
                } else {
                    input.put("docs", result);
                    resultFuture.complete(Collections.singleton(input));
                }
            } else if (getRes.failed()) {
                resultFuture.complete(null);
                return;
            }
        });
    }

}
