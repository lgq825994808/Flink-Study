package com.adb.flink.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 使用固定元素当数据源
 */
public class FlinkSource_Elements {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<? extends Number> dataStreamSource = env.fromElements(2, 4, 6, 9, 23, 46, 26, 5, 7);

        dataStreamSource.print();

        env.execute();
    }
}
