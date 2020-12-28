package com.adb.flink.transform;

import com.adb.flink.dto.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.ArrayList;
import java.util.Collections;

/**
 * 流操作
 *流的切分与合并
 */
public class FlinkTransform_MultipleStreams {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String path = "D:\\ideaWorkSpace\\Flink-Study\\Flink-Demo\\src\\main\\resources\\SensorReading.txt";
        DataStreamSource<String> dataStreamSource = env.readTextFile(path);

        SingleOutputStreamOperator<SensorReading> stream = dataStreamSource.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] split = s.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });
        //流的切分
        SplitStream<SensorReading> split = stream.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading sensorReading) {
                return sensorReading.getTemperature() > 35 ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });
        DataStream<SensorReading> highTempStream = split.select("high");
        DataStream<SensorReading> lowTempStream = split.select("low");
        highTempStream.print("high");
        lowTempStream.print("low");

        // 2. 合流 connect，将高温流转换成二元组类型，与低温流连接合并之后，输出状态信息
        DataStream<Tuple2<String, Double>> warningStream = highTempStream.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), value.getTemperature());
            }
        });
        //合并流
        ConnectedStreams<SensorReading, Tuple2<String, Double>> connect = lowTempStream.connect(warningStream);
        //将流的数据转为统一数据类型
        SingleOutputStreamOperator<SensorReading> allStream = connect.map(new CoMapFunction<SensorReading, Tuple2<String, Double>, SensorReading>() {
            @Override
            public SensorReading map1(SensorReading sensorReading) throws Exception {
                return sensorReading;
            }

            @Override
            public SensorReading map2(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                return new SensorReading(stringDoubleTuple2.f0, System.currentTimeMillis()/1000, stringDoubleTuple2.f1);
            }
        });
        allStream.print("allStream");


        env.execute();
    }
}
