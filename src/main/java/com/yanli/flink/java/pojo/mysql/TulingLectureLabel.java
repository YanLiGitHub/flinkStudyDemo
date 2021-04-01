package com.yanli.flink.java.pojo.mysql;

import lombok.Data;

/**
 * @author YanLi
 * @version 1.0
 * @ClassName: TulingLectureLabel
 * @date 2021/4/1 5:09 下午
 */
@Data
public class TulingLectureLabel {
    /**
     * 讲次id
     */
    private String lectureId;

    /**
     * 讲次名称
     */
    private String lectureName;

    /**
     * 应用树维度id
     */
    private String appTreeLkcId;

    /**
     * 应用树维度名称
     */
    private String appTreeLkcName;

    /**
     * 应用树知识id
     */
    private String appTreeKnowId;

    /**
     * 应用树知识名称
     */
    private String appTreeKnowName;

    /**
     * 5.0标签数id
     */
    private String lkcId;

    /**
     * 5.0末级知识点id列表
     */
    private String knowLinkedList;

    public TulingLectureLabel() {
    }
}
