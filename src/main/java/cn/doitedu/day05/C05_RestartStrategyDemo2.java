package cn.doitedu.day05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 按照一端时间的错误率执行重启策略
 *
 */
public class C05_RestartStrategyDemo2 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //在一段时间内允许的最大重启次数
        //如果在30秒内，重启的次数没有超过3次，程序可以重启
        //如果超过了30秒，重新多出现错误的次数进行计数
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.seconds(30), Time.seconds(2)));

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
        KeyedStream<Tuple2<String, Integer>, String> keyed = wordAndOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {

            @Override
            public String getKey(Tuple2<String, Integer> tp) throws Exception {
                return tp.f0;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = keyed.sum("f1");

        //调用Sink
        summed.print();

        //执行
        env.execute();





    }

}
