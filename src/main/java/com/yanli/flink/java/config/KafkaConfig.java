package com.yanli.flink.java.config;

import com.yanli.flink.java.utils.PropertiesUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author YanLi
 * @version 1.0
 * @ClassName: KafkaConfig
 * @date 2021/2/24 5:07 下午
 *  获取kafka config
 */
public class KafkaConfig {

    private static Properties loadConfigFile(){
        Properties loadConfigFile = PropertiesUtil.getProperties("kafka.properties");
        return loadConfigFile;
    }

    /**
     * 本地链接kafka ssl 配置
     * @return
     */
    private static Properties init(){
        Properties properties = new Properties();
        //设置sasl文件的路径
//        if (null == System.getProperty("java.security.auth.login.config")) { //请注意将XXX修改为自己的路径
//            //这个路径必须是一个文件系统可读的路径，不能被打包到jar中
//            System.setProperty("java.security.auth.login.config", classPath + loadConfigFile().getProperty("java.security.auth.login.config"));
//        }
//        //设置SSL根证书的路径，请记得将XXX修改为自己的路径
//        //与sasl路径类似，该文件也不能被打包到jar中
//        properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, classPath + loadConfigFile().getProperty("ssl.truststore.location"));
//        //根证书store的密码，保持不变
//        properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "KafkaOnsClient");
//        //接入协议，目前支持使用SASL_SSL协议接入
//        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
//        //SASL鉴权方式，保持不变
//        properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
//        // 忽略主机验证
//        properties.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
        return properties;
    }

    /**
     * 获取kafka消费者配置信息
     */
    public static Properties getKafkaConsumerConfig(String bootstrap_servers){
        Properties propConsumer = KafkaConfig.init();
        propConsumer.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
        propConsumer.setProperty(ConsumerConfig.GROUP_ID_CONFIG, loadConfigFile().getProperty("group.id"));
        propConsumer.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, loadConfigFile().getProperty("kafka.consumer.enable-auto-commit"));
        propConsumer.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, loadConfigFile().getProperty("kafka.consumer.auto-commit-interval"));
        propConsumer.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, loadConfigFile().getProperty("kafka.consumer.auto-offset-reset"));

        return propConsumer;

    }

    /**
     * 获取kafka 生产者配置信息
     */
    public static Properties getKafkaProducerConfig(){
        Properties propProducer = KafkaConfig.init();
        propProducer.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, loadConfigFile().getProperty("bootstrap.servers"));
        propProducer.setProperty(ProducerConfig.RETRIES_CONFIG, loadConfigFile().getProperty("kafka.producer.retries"));
        propProducer.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, loadConfigFile().getProperty("kafka.producer.batch-size"));
        propProducer.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, loadConfigFile().getProperty("kafka.producer.buffer-memory"));
        propProducer.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, loadConfigFile().getProperty("kafka.producer.transaction.timeout.ms"));

        return propProducer;
    }
}
