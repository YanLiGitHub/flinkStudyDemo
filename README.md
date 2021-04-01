# flinkStudyDemo

个人记录学习flink 过程中的demo和遇到的问题

2020-01-21：

1、flink streaming api 连接 kafka 和 mysql 

2、flink table api 连接kafka 和 mysql

该部分代码主要由Scala编写，Scala-2.12.8； flink 选择的是 1.9.1

注：由于之前项目中使用blink、kafka和mysql 组件，所以先适应了一下flink 与两个组件的连接，后续尝试其它组件


2020-06-08:

1、flink streaming api 连接 hbase source 和sink

代码主要由Scala 实现，Scala-2.12.8； flink 选择的是 1.9.1

2、修改了new FlinkKafkaProducer（）kafka生产者的创建api，新的api需要用KafkaSerializationSchema，实现接口里的serialize 方法即可。参考了SimpleStringSchema 的实现。

2021-04-01
flink 升级了1.11.1版本

1、Java api 实现 异步I/O访问 MySQL 数据库

2、通过 新的JDBC connect 实现 MySQL sink.
