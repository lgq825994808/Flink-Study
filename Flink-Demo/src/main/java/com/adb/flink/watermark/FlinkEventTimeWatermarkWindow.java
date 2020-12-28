package com.adb.flink.watermark;

import com.adb.flink.dto.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * Event Time：事件创建的时间
 * Ingestion Time：数据进入Flink的时间
 * Processing Time：执行操作算子的本地系统时间，与机器相关 （默认）
 *
 *Watermark：就是设置等待乱序数据的延迟时间
 */
public class FlinkEventTimeWatermarkWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<SensorReading> dataStream = dataStreamSource.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] split = s.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

        // 升序数据设置事件时间和watermark (数据的时间没有乱序)
//                dataStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorReading>() {
//                    @Override
//                    public long extractAscendingTimestamp(SensorReading element) {
//                        return element.getTimestamp() * 1000L;
//                    }
//                })
        //设置时间语义为：Event Time
        // 乱序数据设置时间戳和watermark，设置watermark为2秒 （数据乱序）
        //提取数据里面的时间（对象的“timestamp”字段），用于设置事件时间（必须为毫秒）
        dataStream.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getTimestamp() * 1000L;
            }
        });

        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late") {
        };

        //示例：窗口为0-15秒，watermark为：2秒，allowedLateness为：5秒
        //当来到的数据的时间来到了15秒时，窗口不关闭，也不计算
        //当来到的数据的时间来到了17秒时，窗口开始计算窗口0-15秒之间的数据（此时计算一次，用于等待16-17秒时间内，新数据时间范围在0-15秒之间）
        //当来到的数据时间在17-22秒之间时，来一条数据的时间在0-15秒之间的数据，就重新计算窗口0-15秒的结果（来一条就计算一次）
        // 当来到的数据时间等于22秒时，此时关闭0-15秒的窗口
        //当来到的数据时间大于22秒时，此时如果还有时间是窗口0-15秒之间的数据，如果设置了‘sideOutputLateData',就将该数据放到侧输出流，如果没有设置就直接丢弃

        // 基于事件时间的开窗聚合，统计15秒内温度的最小值
        SingleOutputStreamOperator<SensorReading> minTempStream = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))  //滚动窗口时间范围
                .allowedLateness(Time.seconds(5))  //设置窗口等待延迟数据的时间,在等待时间范围内，每到一条数据就计算一次结果
                .sideOutputLateData(outputTag)   //将迟到的数据输出到侧输出流里面，用于后续统计
                .minBy("temperature");

        minTempStream.print("minTemp");
        minTempStream.getSideOutput(outputTag).print("late");

        env.execute();
    }
}
