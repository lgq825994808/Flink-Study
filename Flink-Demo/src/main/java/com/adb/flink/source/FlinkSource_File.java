package com.adb.flink.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 使用文件当数据源
 */
public class FlinkSource_File {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String path="D:\\ideaWorkSpace\\Flink-Study\\Flink-Demo\\src\\main\\resources\\SensorReading.txt";
        DataStreamSource<String> dataStreamSource = env.readTextFile(path);

        dataStreamSource.print();

        env.execute();
    }
}
