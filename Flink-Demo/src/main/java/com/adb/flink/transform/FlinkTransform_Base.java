package com.adb.flink.transform;

import com.adb.flink.dto.SensorReading;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlinkTransform_Base {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String path="D:\\ideaWorkSpace\\Flink-Study\\Flink-Demo\\src\\main\\resources\\SensorReading.txt";
        DataStreamSource<String> dataStreamSource = env.readTextFile(path);

        SingleOutputStreamOperator<SensorReading> streamMap = dataStreamSource.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] split = s.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

        SingleOutputStreamOperator<String> flatMap = dataStreamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] split = s.split(",");
                for (String s1 : split) {
                    collector.collect(s1);
                }
            }
        });

        SingleOutputStreamOperator<String> filter = dataStreamSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return s.startsWith("source_1");
            }
        });

        streamMap.print("streamMap");
        flatMap.print("flatMap");
        filter.print("filter");

        env.execute();
    }
}
