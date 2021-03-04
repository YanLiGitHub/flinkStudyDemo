package com.yanli.flink.java.pojo.kafka;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;

import java.util.List;

/**
 * @author YanLi
 * @version 1.0
 * @ClassName: BehaviorEvent
 * @date 2021/1/25 下午8:00
 */
@Data
public class BehaviorEvent {

    private String eventId; //事件id
    private String appId;//场景id
    private String orgCode;//机构
    private String requestId;//请求id（推荐id、搜索id）
    private String sourceRequestType;//请求来源类型（推荐/搜索）
    private int gradeCode;//年级
    private int subjectCode;//学科
    private int termCode;//学期
    private String userId;//用户id
    private String identityCode;//身份code
    private long subTime;//提交时间
    private BehaviorParam behaviorParam;//行为字段
    private JSONObject params;//扩展字段

    @Data
    public class BehaviorParam {
        private int behaviorCode;
        private BehaviorContent behaviorContent;
    }

    @Data
    public class BehaviorContent {
        private int elementTypeCode;
        private String elementId;
        private List<AnswerBehavior> queAnswerList;
    }

    @Data
    public class AnswerBehavior {
        private String answerId;
        private String queId;
        private int queSort;
        private String stuId;
        private String stuName;
        private int ansRes;
        private double ansDur;
        private double queScore;
        private double stuScore;
        private String operateType;
        private List<Answer> answer;
    }

    @Data
    public class Answer {
        private String rootId;
        private String queId;
        private double queScore;
        private double stuScore;
        private int ansRes;
        private int type;
        private JSONObject data;
        private JSONObject judge;
    }
}
