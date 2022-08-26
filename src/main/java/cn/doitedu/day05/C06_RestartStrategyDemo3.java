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
 *  开启Checkpoint（定期将state保存起来），并且subtask重启后，可以恢复以前状态
 *
 *  如果开启了checkpoint，默认的重启策略是无限重启
 *
 *  Flink一般都会开启checkpoint,就使用了fixedDelayRestart(Integer.Max_Value, 1000)
 *
 *  如果不想使用默认的策略，还可以自己设置相应的策略。
 *
 */
public class C06_RestartStrategyDemo3 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //开启checkpoint
        //默认的重启策略是fixedDelayRestart(Integer.Max_Value, 1000)
        env.enableCheckpointing(5000); //5秒钟将状态做快照

        //env.setRestartStrategy();

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
