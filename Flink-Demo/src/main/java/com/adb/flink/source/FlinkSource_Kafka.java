package com.adb.flink.source;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * 使用kafka当数据源
 */
public class FlinkSource_Kafka {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.133.131:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);

        DataStreamSource<String> stringDataStreamSource = env.addSource(new FlinkKafkaConsumer011<String>("flink-test", new SimpleStringSchema(), properties));

        stringDataStreamSource.print();

        env.execute();
    }
}
