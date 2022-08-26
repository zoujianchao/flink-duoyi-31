package cn.doitedu.day08;

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
 * 演示使用定时器(Processing)
 */
public class C06_ProcessFunctionTimerDemo1 {

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
        keyedStream.process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                //注册定时器
                long currentTime = ctx.timerService().currentProcessingTime();
                long triggerTime = currentTime + 20000;
                System.out.println("定时器在： " + currentTime + "被注册，触发时间是：" + triggerTime);
                ctx.timerService().registerProcessingTimeTimer(triggerTime);
            }

            //当ProcessingTime >= 注册的定时器的是，onTimer方法被调用
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                System.out.println("OnTime方法被调用：" + timestamp + " , 对应的key是：" + ctx.getCurrentKey());
            }
        });

        env.execute();


    }


}

