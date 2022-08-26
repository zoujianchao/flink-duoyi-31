package cn.doitedu.day08;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 将当前窗口中的数据进行增量聚合，然后再将窗口聚合后的结果进行与历史状态进行聚合
 *
 * 如果一条一条的聚合，实时性高，但是每来一条数据都有输出一条结果，输出到外部的存储系统，会增大写入的压力
 *
 * 划分窗口后，可以调用reduce方法（reduce方法输入的类型和输出的类型必须一致）
 *
 */
public class C03_WindowAggregateWithHistoryState {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(20000);

        //spark,2
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> tpStream = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
            }
        });

        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = tpStream
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        //先调用ReduceFunction进行增量聚合，窗口触发后再调用WindowFunction进行计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = windowedStream.aggregate(new MyAggregateFunction(), new MyProcessWindowFunction());

        res.print();

        env.execute();

    }

    public static class MyAggregateFunction implements AggregateFunction<Tuple2<String, Integer>, Integer, Integer> {

        //每个窗口，每个key的初始值
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        //每个key输入一次，会调用一次add方法
        @Override
        public Integer add(Tuple2<String, Integer> in, Integer accumulator) {
            return accumulator + in.f1;
        }

        //窗户触发后，返回的结果
        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        //只有会话窗口可能会调用该方法
        @Override
        public Integer merge(Integer a, Integer b) {
            return null;
        }
    }

    public static class MyProcessWindowFunction extends ProcessWindowFunction<Integer, Tuple2<String, Integer>, String, TimeWindow> {

        private transient ValueState<Integer> historyState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //初始化或恢复状态
            ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<Integer>("history-state", Integer.class);
            historyState = getRuntimeContext().getState(stateDescriptor);
        }

        /**
         * 窗口触发后，没有key都会调用一次process方法
         * @param key
         * @param context
         * @param input
         * @param out
         * @throws Exception
         */
        @Override
        public void process(String key, Context context, Iterable<Integer> input, Collector<Tuple2<String, Integer>> out) throws Exception {
            //可以获取窗口的信息
            //context.window().getEnd()

            Integer count = input.iterator().next();

            //获取历史数据
            Integer historyCount = historyState.value();
            if (historyCount == null) {
                historyCount = 0;
            }
            count += historyCount;
            historyState.update(count);

            out.collect(Tuple2.of(key, count));
        }
    }

}
