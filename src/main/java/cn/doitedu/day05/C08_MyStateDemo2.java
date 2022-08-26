package cn.doitedu.day05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * 不使用Flink的状态编程API，而是自己使用特殊的集合保存中间结果数据
 *
 * 深入理解Flink的状态是什么？
 */
public class C08_MyStateDemo2 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 1000));
        env.enableCheckpointing(5000);

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);



        //调用Transformation(s)
        SingleOutputStreamOperator<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                String[] words = line.split(" ");

                for (String word : words) {
                    //人为制造异常
                    if(word.startsWith("error")) {
                        throw new RuntimeException("数据有问题，出现了异常!");
                    }
                    collector.collect(word);
                }

            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String w) throws Exception {
                return Tuple2.of(w, 1);
            }
        });

        //分区聚合
        //key相同的一定进入到同一个分区，但是同一个分区中，可能会有多个不同的key
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordAndOne.keyBy(t -> t.f0);

        //keyedStream.sum(1)
        //自己实现类似sum的功能
        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = keyedStream.map(new MySumFunction());

        //调用Sink
        summed.print();

        //执行
        env.execute();
    }

    /**
     * 我自己定义实现sum功能的Function
     * 1.能不能实现正确累加(能)
     * 2.能不能容错（不能）
     */
    public static class MySumFunction implements MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

        private HashMap<String, Integer> word2Count = new HashMap<String, Integer>();

        @Override
        public Tuple2<String, Integer> map(Tuple2<String, Integer> input) throws Exception {
            String word = input.f0;
            //根据单词到Map中取数据
            Integer count = word2Count.get(word);
            if (count == null) {
                count = 0;
            }
            count += input.f1;
            //更新中间结果
            word2Count.put(word, count);
            //输出结果
            return Tuple2.of(word, count);
        }
    }

}
