package com.yanli.flink.myDefindFunctions;


import com.yanli.flink.pojo.QuestionKnowledeg;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @author yanli
 * @version 1.0
 * @date 2019/12/22 15:58
 * 自定义pojo类型聚和count
 */
public class CountAgg implements AggregateFunction<QuestionKnowledeg,Long,Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(QuestionKnowledeg questionKnowledeg, Long accumulator) {
        return accumulator+1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long aLong, Long acc1) {
        return aLong+acc1;
    }
}
