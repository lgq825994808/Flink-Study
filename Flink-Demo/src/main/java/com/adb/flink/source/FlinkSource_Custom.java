package com.adb.flink.source;

import com.adb.flink.dto.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.text.DecimalFormat;
import java.util.Properties;
import java.util.Random;

/**
 * 自定义数据源
 */
public class FlinkSource_Custom {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //使用自定义数据源
        DataStreamSource<SensorReading> streamSource = env.addSource(new MyCustomSource());

        streamSource.print();

        env.execute();
    }

    public static class MyCustomSource implements SourceFunction<SensorReading>{

        // 定义一个标识位，用来控制数据的产生
        private boolean running = true;

        @Override
        public void run(SourceContext<SensorReading> sourceContext) throws Exception {
            Random random = new Random();
            DecimalFormat df = new DecimalFormat("#.00");

            while (running){
                String format = df.format(35 + random.nextDouble() * 10);
                sourceContext.collect(new SensorReading("source_"+random.nextInt(10),System.currentTimeMillis()/1000,Double.parseDouble(format)));
                Thread.sleep(1000);
            }

        }

        @Override
        public void cancel() {
            running=false;
        }
    }
}

