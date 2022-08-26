package cn.doitedu.day08;

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
import org.apache.flink.util.Collector;

/**
 * ProcessFunction是Flink更加底层的方法，可以访问Flink程序底层的属性和方法
 * 优点：更灵活
 * 缺点：使用起来更复杂
 * <p>
 * ProcessFunction有三种功能
 * 1.对数据进行一条一条的处理
 * 2.对KeyedStream使用KeyedState
 * 3.对KeyedStream使用定时器（可以实现类似窗口的功能）
 * <p>
 * 演示使用定时器(ProcessingTime) + 状态，实现类似滚动窗口的功能
 * OnTime方法触发时机：当前的系统时间 >= 注册的定期时间（每个key都可以注册自己的定时器）
 *
 */
public class C07_ProcessFunctionTimerDemo2 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000));
        env.enableCheckpointing(5000);

        //spark,1
        //hive,3
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //对数据进行整理
        SingleOutputStreamOperator<Tuple2<String, Integer>> tpStream = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
            }
        });

        //keyBy
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = tpStream.keyBy(t -> t.f0);

        //使用ProcessFunction + 定时器
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = keyedStream.process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {

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
                //注册定时器
                long currentTime = ctx.timerService().currentProcessingTime();
                //15:10:35 -> 注册的定时器触发时间 15:11:00
                //15:10:36 -> 注册的定时器触发时间 15:11:00
                //1210000 % 30000 = 10000
                //1210000 - 10000 = 1200000
                //1200000 + 30000 = 1230000
                long triggerTime = currentTime - currentTime % 30000 + 30000;
                System.out.println("定时器在： " + currentTime + "被注册，触发时间是：" + triggerTime);
                //同一个key，注册了多个时间相同的定时器，仅会触发一次（如果key，并且时间相同，后注册的会覆盖以前注册的）
                ctx.timerService().registerProcessingTimeTimer(triggerTime);
                //（不直接输出）实现增量聚合
                Integer historyCount = valueState.value();
                if (historyCount == null) {
                    historyCount = 0;
                }
                historyCount += input.f1;
                //更新状态
                valueState.update(historyCount);
            }

            //当ProcessingTime >= 注册的定时器的是，onTimer方法被调用
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                System.out.println("OnTime方法被调用：" + timestamp + " , 对应的key是：" + ctx.getCurrentKey());
                Integer count = valueState.value();
                out.collect(Tuple2.of(ctx.getCurrentKey(), count));
                valueState.update(null); //只累加当前窗口中的数据
            }
        });

        res.print();

        env.execute();


    }


}

