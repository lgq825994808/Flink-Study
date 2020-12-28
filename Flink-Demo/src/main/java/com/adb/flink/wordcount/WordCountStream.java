package com.adb.flink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 流式处理  统计单词个数
 */
public class WordCountStream {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取文件内容
       /* String path="D:\\ideaWorkSpace\\Flink-Study\\Flink-Demo\\src\\main\\resources\\data.txt";
        DataStream<String> dataSource = env.readTextFile(path);*/

       //cmd 窗口使用命令 nc -l -p 8888 发送数据
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        Integer prot = parameterTool.getInt("prot");
        DataStreamSource<String> dataSource = env.socketTextStream(host, prot);

        //将读取的数据内容转换成二元组（hello,1）这样的类型
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] split = s.split(" ");
                for (String str : split) {
                    collector.collect(new Tuple2<>(str, 1));
                }
            }
        });
        //按元组的第一个角标分组
        KeyedStream<Tuple2<String, Integer>, Tuple> tuple2TupleKeyedStream = flatMap.keyBy(0);
        //按元组的第二个角标累加
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = tuple2TupleKeyedStream.sum(1);
        //打印数据
        sum.print();
        //开始执行
        env.execute();
    }
}
