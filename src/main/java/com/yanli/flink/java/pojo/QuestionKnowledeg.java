package com.yanli.flink.java.pojo;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * @author yanli
 * @version 1.0
 * @date 2019/12/22 16:06
 * 自定义 TypeInfoFactory ，待完成
 */
@TypeInfo(MyTypeInfoFactory.class)
public class QuestionKnowledeg {
    private String queId;
    private String stuId;
    private int queSort;
    private double queScore;
}

class MyTypeInfoFactory extends TypeInfoFactory<QuestionKnowledeg>{

    @Override
    public TypeInformation<QuestionKnowledeg> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
        return null;
    }
}

class MyTypeInfo extends TypeInformation<QuestionKnowledeg>{
    private TypeInformation queId;
    private TypeInformation stuId;
    private TypeInformation queSort;
    private TypeInformation queScore;

    public TypeInformation getQueId() {
        return queId;
    }

    public TypeInformation getStuId() {
        return stuId;
    }

    public TypeInformation getQueSort() {
        return queSort;
    }

    public TypeInformation getQueScore() {
        return queScore;
    }

    public MyTypeInfo(TypeInformation queId, TypeInformation stuId, TypeInformation queSort, TypeInformation queScore) {
        this.queId = queId;
        this.stuId = stuId;
        this.queSort = queSort;
        this.queScore = queScore;
    }

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 0;
    }

    @Override
    public int getTotalFields() {
        return 0;
    }

    @Override
    public Class<QuestionKnowledeg> getTypeClass() {
        return QuestionKnowledeg.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<QuestionKnowledeg> createSerializer(ExecutionConfig config) {
        return null;
    }

    @Override
    public String toString() {
        return null;
    }

    @Override
    public boolean equals(Object obj) {
        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean canEqual(Object obj) {
        return false;
    }
}
