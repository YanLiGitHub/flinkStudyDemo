package com.yanli.flink.java.streamingApi.hdfs;

import com.yanli.flink.java.pojo.hdfs.Person;
import com.yanli.flink.java.streamingApi.hdfs.orc.PersonVectorizer;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.orc.writer.OrcBulkWriterFactory;
import org.apache.flink.runtime.util.HadoopUtils;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author YanLi
 * @version 1.0
 * @ClassName: FlinkConnectHDFS
 * @date 2021/4/15 1:44 下午
 */
public class FlinkConnectHDFS {
    private static final Logger logger = LoggerFactory.getLogger(FlinkConnectHDFS.class);

    //Flink 1.11版本使用 StreamingFileSink，通过checkpoint 保证数据一致性
    // Flink 1.12版本 之后可以使用 FileSink
    public static StreamingFileSink addStringSink(String outputPath){
        StreamingFileSink<String> streamingFileSink = StreamingFileSink.forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>())
                .withRollingPolicy(DefaultRollingPolicy.builder() //自定义滚动策略
                        .withMaxPartSize(1024L * 1024L * 1024L) //文件大小达到1G 默认128M
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(5)) //最近 5 分钟没有收到新的记录，默认1分钟
                        .withRolloverInterval(TimeUnit.MINUTES.toMillis(15)) //最少15 分钟的滚动一次，默认1分钟
                        .build()
                ).withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd")) // 分桶规则,按天分；默认小时 yyyy-MM-dd--HH 分
                .build();

        return streamingFileSink;
    }

    //ORC格式批量写到file
    public static StreamingFileSink addOrcSink(String outputPath){
        String schema = "struct<_col0:string,_col1:int>";

        Properties writerProperties = new Properties();
        writerProperties.setProperty("orc.compress", "snappy");

        //按person 的年龄分桶
        BucketAssigner<Person, String> personStringBucketAssigner = new BucketAssigner<Person, String>() {
            @Override
            public String getBucketId(Person element, Context context) {
                return String.valueOf(element.getAge());
            }

            @Override
            public SimpleVersionedSerializer<String> getSerializer() {
                return SimpleVersionedStringSerializer.INSTANCE;
            }
        };

        Configuration hadoopConf = HadoopUtils.getHadoopConfiguration(GlobalConfiguration.loadConfiguration());
        OrcBulkWriterFactory<Person> personOrcBulkWriterFactory = new OrcBulkWriterFactory<>(new PersonVectorizer(schema),writerProperties,hadoopConf);

        StreamingFileSink<Person> streamingFileSink = StreamingFileSink.forBulkFormat(new Path(outputPath), personOrcBulkWriterFactory)
                .withRollingPolicy(OnCheckpointRollingPolicy.build()) // checkpoint 成功后, pending 的文件会变成 Finished
                .build();

        return streamingFileSink;
    }
}
