package cn.doitedu.day08;

import org.apache.flink.api.common.functions.AbstractRichFunction;
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
 */
public class C02_WindowReduceWithHistoryState {


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
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = windowedStream.reduce(new MyReduceFunction(), new MyWindowFunction());

        res.print();

        env.execute();

    }

    public static class MyReduceFunction implements ReduceFunction<Tuple2<String, Integer>> {


        @Override
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
            value1.f1 = value1.f1 + value2.f1;
            return value1;
        }
    }

    //窗口触发后（调用完WindowOperator后），还可以调用MyWindowFunction
    //public static class MyWindowFunction extends AbstractRichFunction implements WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow> {

    public static class MyWindowFunction extends AbstractRichFunction implements WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow> {


        private transient ValueState<Integer> historyState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //初始化或恢复状态
            ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<Integer>("history-state", Integer.class);
            historyState = getRuntimeContext().getState(stateDescriptor);
        }

        /**
         * 窗口触发后，会将WindowOperator（MyReduceFunction）增量聚合的结果，每个key都会调用一次该方法
         * @param key
         * @param window
         * @param input
         * @param out
         * @throws Exception
         */
        @Override
        public void apply(String key, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
            //取出当前窗口中聚合的结果
            Tuple2<String, Integer> tp = input.iterator().next();

            Integer historyCount = historyState.value();
            if (historyCount == null) {
                historyCount = 0;
            }
            historyCount += tp.f1;
            //更新
            historyState.update(historyCount);

            tp.f1 = historyCount;
            //输出结果
            out.collect(tp);
        }
    }


    public static class MyWindowFunction2 extends ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {

        }
    }

}
