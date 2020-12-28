package com.adb.flink.source;

import com.adb.flink.dto.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 使用集合当数据源
 */
public class FlinkSource_List {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<SensorReading> arrayList = new ArrayList<>();
        arrayList.add(new SensorReading("source_1",1607406824l,36.4));
        arrayList.add(new SensorReading("source_2",1607406835l,36.7));
        arrayList.add(new SensorReading("source_3",1607406868l,36.9));
        arrayList.add(new SensorReading("source_2",1607406811l,37.4));
        arrayList.add(new SensorReading("source_1",1607406524l,32.4));

        //使用集合当数据源
        DataStreamSource<SensorReading> dataStreamSource = env.fromCollection(arrayList);

        //打印
        dataStreamSource.print();

        //开始执行
        env.execute();
    }
}
