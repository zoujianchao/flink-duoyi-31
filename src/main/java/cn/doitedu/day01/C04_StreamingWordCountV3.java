package cn.doitedu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class C04_StreamingWordCountV3 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //调用Source方法创建DataStream
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        lines.flatMap(new LineSplitor())
                .keyBy(0)
                .sum(1)
                .print();


        //执行
        env.execute();


    }


    public static class LineSplitor implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            for (String word : s.split(" ")) {
                collector.collect(Tuple2.of(word, 1));
            }
        }
    }


}
