package com.yanli.flink.java.streamingApi.hdfs.orc;

import com.yanli.flink.java.pojo.hdfs.Person;
import org.apache.flink.orc.vector.Vectorizer;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;

/**
 * @author YanLi
 * @version 1.0
 * @ClassName: Person
 * @date 2021/4/15 2:47 下午
 * 实体类转换成 VectorizedRowBatch
 */
public class PersonVectorizer extends Vectorizer<Person> implements Serializable {
    public PersonVectorizer(String schema) {
        super(schema);
    }

    @Override
    public void vectorize(Person person, VectorizedRowBatch vectorizedRowBatch) throws IOException {
        BytesColumnVector name = (BytesColumnVector) vectorizedRowBatch.cols[0];
        LongColumnVector age = (LongColumnVector) vectorizedRowBatch.cols[1];
        int row = vectorizedRowBatch.size++;
        name.setVal(row,person.getName().getBytes(StandardCharsets.UTF_8));
        age.vector[row] = person.getAge();
    }
}
