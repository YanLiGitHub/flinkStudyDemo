package com.yanli.flink.java.pojo.hbase;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;

/**
 * @author YanLi
 * @version 1.0
 * @ClassName: ZKWisdomAnswerDetial
 * @date 2021/3/3 11:21 上午
 */
@Data
public class ZKWisdomAnswerDetial {
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
    private int behaviorCode; //行为code
    private int elementTypeCode; // 元素类型code
    private String elementId; //元素id
    private String answerId; // 作答记录id
    private String queId; //试题id
    private int queSort; //试题序号
    private String stuId; //学生id
    private String stuName; //学生名称
    private int ansRes; //作答结果
    private double ansDur;//作答时长
    private double queScore;//试题分数
    private double stuScore;//学生得分
    private String operateType;//操作类型（insert,update）
    private String rootId; // 大题id
    private String subQueId;//子题id
    private double subQueScore; //子题分值
    private double subStuScore;//子题得分
    private int subAnsRes;//子题作答结果
    private int subType;//试题类型
    private JSONObject data; //作答结果
    private JSONObject judge;//判题结果
    private JSONObject params;//扩展字段

    @Override
    public String toString() {
        return "ZKWisdomAnswerDetial{" +
                "eventId='" + eventId + '\'' +
                ", appId='" + appId + '\'' +
                ", orgCode='" + orgCode + '\'' +
                ", requestId='" + requestId + '\'' +
                ", sourceRequestType='" + sourceRequestType + '\'' +
                ", gradeCode=" + gradeCode +
                ", subjectCode=" + subjectCode +
                ", termCode=" + termCode +
                ", userId='" + userId + '\'' +
                ", identityCode='" + identityCode + '\'' +
                ", subTime=" + subTime +
                ", behaviorCode=" + behaviorCode +
                ", elementTypeCode=" + elementTypeCode +
                ", elementId='" + elementId + '\'' +
                ", answerId='" + answerId + '\'' +
                ", queId='" + queId + '\'' +
                ", queSort=" + queSort +
                ", stuId='" + stuId + '\'' +
                ", stuName='" + stuName + '\'' +
                ", ansRes=" + ansRes +
                ", ansDur=" + ansDur +
                ", queScore=" + queScore +
                ", stuScore=" + stuScore +
                ", operateType='" + operateType + '\'' +
                ", rootId='" + rootId + '\'' +
                ", subQueId='" + subQueId + '\'' +
                ", subQueScore=" + subQueScore +
                ", subStuScore=" + subStuScore +
                ", subAnsRes=" + subAnsRes +
                ", subType=" + subType +
                ", data=" + data +
                ", judge=" + judge +
                ", params=" + params +
                '}';
    }
}
