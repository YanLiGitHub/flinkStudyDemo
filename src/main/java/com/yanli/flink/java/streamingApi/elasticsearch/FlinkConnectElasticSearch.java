package com.yanli.flink.java.streamingApi.elasticsearch;


import com.yanli.flink.java.utils.JavaJsonUtil;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch.util.NoOpFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.util.ExceptionUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author YanLi
 * @version 1.0
 * @ClassName: FlinkConnectElasticSearch
 * @date 2021/4/14 4:15 下午
 */
public class FlinkConnectElasticSearch {

    private static final Logger logger = LoggerFactory.getLogger(FlinkConnectElasticSearch.class);

    public static <T> ElasticsearchSink addSink(List<HttpHost> httpHosts,int bulkFlushMaxActions){

        //自定义失败请求处理方式
        ActionRequestFailureHandler failureHandler = new ActionRequestFailureHandler(){
            @Override
            public void onFailure(ActionRequest action, Throwable failure, int restStatusCode, RequestIndexer indexer) throws Throwable {
                if (ExceptionUtils.findThrowable(failure, EsRejectedExecutionException.class).isPresent()) {
                    // 由于队列容量饱和而失败的请求重新添加到接收器
                    indexer.add(action);
                } else if (ExceptionUtils.findThrowable(failure, ElasticsearchParseException.class).isPresent()) {
                    // 丢弃格式错误的文档的请求，不会使接收器失败
                    logger.info("格式不正确的文档" + action);
                } else {
                    // 其它错误请求直接失败
                    throw failure;
                }
            }
        };

        //把数据转换成json 创建发送请求
        ElasticsearchSinkFunction<T> function = new ElasticsearchSinkFunction<T>() {
            public IndexRequest createIndexRequest(T element) {
                Map<String, String> json = new HashMap<>();
                json.put("data", JavaJsonUtil.getJson(element));

                return Requests.indexRequest()
                        .index("my-index")
                        .type("")
                        .source(json);
            }

            @Override
            public void process(T element, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                requestIndexer.add(createIndexRequest(element));
            }
        };


        ElasticsearchSink.Builder esSinkBuilder = new ElasticsearchSink.Builder<T>(httpHosts, function);

        //批次插入 x条数据插入es一次
        esSinkBuilder.setBulkFlushMaxActions(bulkFlushMaxActions);
        esSinkBuilder.setFailureHandler(failureHandler);
        ElasticsearchSink elasticsearchSink = esSinkBuilder.build();
        return elasticsearchSink;
    }

}
