package cn.doitedu.day05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * Flink中有两种状态
 * 1. KeyedState， keyBy后使用的状态，数据是跟key绑定再一起的
 * 2. OperatorState，没有跟key绑定再一起
 * <p>
 * 自己使用Flink的KeyedState实现类似WordCount的功能
 */
public class C09_KeyedStateDemo1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000));
        env.enableCheckpointing(5000);

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //调用Transformation(s)
        SingleOutputStreamOperator<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                String[] words = line.split(" ");

                for (String word : words) {
                    //人为制造异常
                    if (word.startsWith("error")) {
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
        //自己使用状态API实现类似sum的功能
        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = keyedStream.map(new MyStateSumFunction());

        //调用Sink
        summed.print();

        //执行
        env.execute();
    }

    /**
     * 自己定义一个带状态，可以容错的聚合Function，实现类似sum的功能
     * <p>
     * //使用Flink的状态编程API
     */
    public static class MyStateSumFunction extends RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

        private ValueState<Integer> valueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //设置TTL相关参数
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.seconds(30))
                    //设置TTL的更新方式，当创建或修改对应key的数据是，会重新计时
                    //.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) //默认的
                    //.setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite) //创建、修改或读取都会重启计时
                    //设置状态的key见性,只要状态超时了，就不可见了
                    //.setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) //默认的
                    //.setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp) //只要状态没有被清除，就可以获取到
                    .build();
            //1.定义一个状态描述器（状态的名称、类型）
            ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("count-state", Integer.class);
            stateDescriptor.enableTimeToLive(ttlConfig); //为状态设置存活时间

            //2.根据状态描述器初始化或恢复状态
            valueState = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public Tuple2<String, Integer> map(Tuple2<String, Integer> input) throws Exception {

            //根据key到状态中去历史数据
            Integer historyCount = valueState.value();
            if (historyCount == null) {
                historyCount = 0;
            }
            //跟状态进行累加
            historyCount += input.f1;
            //更新状态
            valueState.update(historyCount);

            //输出数据
            input.f1 = historyCount;
            return input;
        }
    }

}
