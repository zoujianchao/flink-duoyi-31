package cn.doitedu.day08;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * ProcessFunction是Flink更加底层的方法，可以访问Flink程序底层的属性和方法
 * 优点：更灵活
 * 缺点：使用起来更复杂
 *
 * ProcessFunction有三种功能
 * 1.对数据进行一条一条的处理
 * 2.对KeyedStream使用KeyedState
 * 3.对KeyedStream使用定时器（可以实现类似窗口的功能）
 *
 * 演示使用KeyedState
 */
public class C05_ProcessFunctionDemo2 {

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

        SingleOutputStreamOperator<Tuple2<String, Integer>> res = keyedStream.process(new MyKeyedProcessFunction());

        res.print();

        env.execute();


    }

    /**
     * 使用ProcessFunction 结合 KeyedState
     */
    public static class MyKeyedProcessFunction extends KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>> {

        private ValueState<Integer> valueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //1.定义一个状态描述器（状态的名称、类型）
            ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("count-state", Integer.class);
            //2.根据状态描述器初始化或恢复状态
            valueState = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public void processElement(Tuple2<String, Integer> input, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

            //根据key到状态中去历史数据
            Integer historyCount = valueState.value();
            if(historyCount == null) {
                historyCount = 0;
            }
            //跟状态进行累加
            historyCount += input.f1;
            //更新状态
            valueState.update(historyCount);

            //输出数据
            input.f1 = historyCount;

            out.collect(input);
        }


    }

}
