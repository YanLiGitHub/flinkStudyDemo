package com.yanli.flink.java.streamingApi.mysql;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.yanli.flink.java.config.MysqlConfig;
import com.yanli.flink.java.pojo.mysql.TulingLectureLabel;
import com.yanli.flink.java.utils.JavaJsonUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author YanLi
 * @version 1.0
 * @ClassName: SampleAsyncDatabase
 * @date 2021/3/19 5:29 下午
 * 通过异步IO读取外部数据库数据
 */
public class SampleAsyncDatabase extends RichAsyncFunction<TulingLectureLabel, TulingLectureLabel>{

    private static final Logger logger = LoggerFactory.getLogger(SampleAsyncDatabase.class);

    private transient SQLClient mySQLClient;
    private Cache<String, TulingLectureLabel> Cache;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Cache = Caffeine
                .newBuilder()
                .maximumSize(1025)
                .expireAfterAccess(10, TimeUnit.MINUTES)
                .build();
        JsonObject mySQLClientConfig = new JsonObject();
        mySQLClientConfig.put("url", MysqlConfig.DB_URL)
                .put("driver_class", MysqlConfig.DRIVER_CLASS)
                .put("max_pool_size", 20)
                .put("user", MysqlConfig.USER_NAME)
//                    .put("max_idle_time",1000)
                .put("password", MysqlConfig.PASSWORD);

        VertxOptions vo = new VertxOptions();
        //使用的Event Loop线程的数量
        vo.setEventLoopPoolSize(10);
        //Worker线程的最大数量
        vo.setWorkerPoolSize(20);

        Vertx vertx = Vertx.vertx(vo);
        mySQLClient = JDBCClient.createShared(vertx, mySQLClientConfig);
    }

    @Override
    public void close() throws Exception {
        logger.info("async function for java close ...");
        super.close();
        if (mySQLClient != null)
            mySQLClient.close();
        if (Cache != null)
            Cache.cleanUp();
    }

    @Override
    public void asyncInvoke(TulingLectureLabel tulingLectureLabel, ResultFuture<TulingLectureLabel> resultFuture) throws Exception {
        String lectureId = tulingLectureLabel.getLectureId();
        //先获取本地缓存数据，如果当前讲次能在缓存里查询到，直接返回
        TulingLectureLabel cacheIfPresent = Cache.getIfPresent(lectureId);
        if (cacheIfPresent != null) {
            resultFuture.complete(Collections.singleton(cacheIfPresent));
            return;
        }
        mySQLClient.getConnection(conn -> {
            if (conn.failed()) {
                //从连接池中获取连接，如果连接已经断开，抛异常
                resultFuture.completeExceptionally(conn.cause());
                return;
            }
            //获取连接池中的conn
            final SQLConnection connection = conn.result();
            /*
                结合自己的查询逻辑，拼凑出相应的sql，然后返回结果。
             */
            String querySql = "select lecture_name,app_tree_lkc_id,app_tree_lkc_name,app_tree_know_id,app_tree_know_name,lkc_id,know_linked_list from tuling_lecture_label where lecture_id = "+ lectureId;


            connection.query(querySql, res2 -> {
                //如果查询失败，返回nul
                if (res2.failed()) {
                    resultFuture.complete(null);
                    return;
                }
                //如果查询成功，通过AsyncResult 获取到 ResultSet
                if (res2.succeeded()) {
                    ResultSet resultSet = res2.result();
                    List<JsonObject> rows = resultSet.getRows();
                    if (rows.size() <= 0) {
                        resultFuture.complete(null);
                        return;
                    }
                    for (JsonObject json : rows) {
                        TulingLectureLabel lectureLabel = json.mapTo(TulingLectureLabel.class);
                        Cache.put(lectureId, lectureLabel);
                        resultFuture.complete(Collections.singleton(lectureLabel));
                        logger.info("execute query : " + querySql + "-2-  return :" + JavaJsonUtil.getJson(lectureLabel));

                    }
                } else {
                    resultFuture.complete(null);
                }
            });

            //关闭连接
            connection.close(done -> {
                if (done.failed()) {
                    throw new RuntimeException(done.cause());
                }
            });

        });

    }

    @Override
    public void timeout(TulingLectureLabel tulingLectureLabel, ResultFuture<TulingLectureLabel> resultFuture) throws Exception {
        super.timeout(tulingLectureLabel,resultFuture);
    }
}
