package com.adb.flink.transform;

import com.adb.flink.dto.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class FlinkTransform__Partition {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 从文件读取数据
        String path="D:\\ideaWorkSpace\\Flink-Study\\Flink-Demo\\src\\main\\resources\\SensorReading.txt";
        DataStream<String> inputStream = env.readTextFile(path);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //dataStream.print("input");

        //1.分区元素循环，每个分区创建相等的负载。数据发生倾斜的时候可以用于性能优化
        //对数据集进行再平衡，重分组，消除数据倾斜
        //DataStream<SensorReading> rebalance = dataStream.rebalance();

        //2.rescale与rebalance很像，也是将数据均匀分布到各下游各实例上，但它的传输开销更小，因为rescale并不是将每个数据轮询地发送给下游每个实例，而是就近发送给下游实例
        //DataStream<SensorReading> rescale = dataStream.rescale();

        // 3. shuffle  将数据随机分发到下一个算子的分区
        //DataStream<SensorReading> shuffleStream = dataStream.shuffle();

        //4.即广播变量。将数据分发到每一个JVM进程，供当前进程的所有线程共享数据。
        //DataStream<SensorReading> broadcast = dataStream.broadcast();

        //5.自定义分区
        SingleOutputStreamOperator<Tuple2<String,Double>> streamOperator = dataStream.map(new MapFunction<SensorReading, Tuple2<String,Double>>() {
            @Override
            public Tuple2<String,Double> map(SensorReading sensorReading) throws Exception {
                return new Tuple2(sensorReading.getId(), sensorReading.getTemperature());
            }
        });
        DataStream<Tuple2<String,Double>> partitionCustom = streamOperator.partitionCustom(new MyPartitioner(),0);
        partitionCustom.print("partitionCustom");

        // 6. keyBy
        //按实体的某个字段分组，（同一个key的数据会分到同一个分区）
//      dataStream.keyBy("id").print("keyBy");

        // 7. global 只会将数据输出到下游算子的第一个实例(一个分区)，简单暴力。
        //DataStreamSink<SensorReading> global = dataStream.global().print("global");

        env.execute();
    }

    /**
     * 自定义分区
     */
    public static class MyPartitioner implements Partitioner<String> {

        @Override
        public int partition(String str, int i) {
            if(str.equals("source_1")){
                return 0;
            }else if(str.equals("source_2")){
                return 1;
            }
            return 2;
        }
    }
}
