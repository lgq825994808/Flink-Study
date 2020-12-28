package com.adb.flink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 批处理方式 统计单词个数
 */
public class WordCountSet {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String path="D:\\ideaWorkSpace\\Flink-Study\\Flink-Demo\\src\\main\\resources\\data.txt";
        //读取文件内容
        DataSet<String> dataSource = env.readTextFile(path);
        //将读取的数据内容转换成二元组（hello,1）这样的类型
        FlatMapOperator<String, Tuple2<String, Integer>> flatMap = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] split = s.split(" ");
                for (String str : split) {
                    collector.collect(new Tuple2<>(str, 1));
                }
            }
        });
        //将二元组的内容按第一个字段分组
        UnsortedGrouping<Tuple2<String, Integer>> tuple2UnsortedGrouping = flatMap.groupBy(0);
        //将二元组的内容按第二个字段累加
        AggregateOperator<Tuple2<String, Integer>> sum = tuple2UnsortedGrouping.sum(1);

        //打印数据
        sum.print();
    }
}
