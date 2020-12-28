package com.adb.flink.transform;

import com.adb.flink.dto.SensorReading;
import com.adb.flink.source.FlinkSource_Custom;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *分组 聚合统计
 */
public class FlinkTransform_RollingAggregation {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String path = "D:\\ideaWorkSpace\\Flink-Study\\Flink-Demo\\src\\main\\resources\\SensorReading.txt";
        DataStreamSource<String> dataStreamSource = env.readTextFile(path);

        SingleOutputStreamOperator<SensorReading> streamMap = dataStreamSource.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] split = s.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

        KeyedStream<SensorReading, Tuple> idKeyedStream = streamMap.keyBy("id");
        //KeyedStream<SensorReading, String> idKeyedStream = streamMap.keyBy(SensorReading::getId);

        //max方法：只获取SensorReading对象的该字段的最大值，其余字段会一直为第一个元素的固定值
        //SingleOutputStreamOperator<SensorReading> temperature = idKeyedStream.max("temperature");
        //maxBy方法：会获取SensorReading对象的该字段的最大值的那个对象的所有记录
        SingleOutputStreamOperator<SensorReading> maxTemperature = idKeyedStream.maxBy("temperature");

        //求和所有温度
        SingleOutputStreamOperator<SensorReading> sumTemperature = idKeyedStream.sum("temperature");

        //聚合统计最大时间和最大温度
        SingleOutputStreamOperator<SensorReading> reduceTemperature = idKeyedStream.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading t1, SensorReading t2) throws Exception {
                long timestamp = t1.getTimestamp().longValue() - t2.getTimestamp().longValue();
                return new SensorReading(t1.getId(), timestamp > 0 ? t1.getTimestamp() : t2.getTimestamp(), Math.max(t1.getTemperature(), t2.getTemperature()));
            }
        });


        maxTemperature.print("maxTemperature");
        sumTemperature.print("sumTemperature");
        reduceTemperature.print("reduceTemperature");

        env.execute();

    }
}
