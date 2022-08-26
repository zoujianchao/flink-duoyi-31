package cn.doitedu.day08;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

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
 * 演示使用定时器(EventTime) + 状态，实现类似滚动窗口的功能
 * OnTime方法触发时机：WaterMark >= 注册的定期时间
 */
public class C08_ProcessFunctionTimerDemo3 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1000,spark,1
        //2000,spark,2
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //使用新的API生成WaterMark
        SingleOutputStreamOperator<String> linesWithWaterMark = lines.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner(new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String element, long recordTimestamp) {
                String[] fields = element.split(",");
                return Long.parseLong(fields[0]);
            }
        }));

        //对数据进行整理
        SingleOutputStreamOperator<Tuple2<String, Integer>> tpStream = linesWithWaterMark.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[1], Integer.parseInt(fields[2]));
            }
        });
        //先keyBy
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = tpStream.keyBy(t -> t.f0);

        //使用processFunction + ListState（KeyedState），计算一段时间内相同单词次数最大的TopN
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = keyedStream.process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            private transient ListState<Tuple2<String, Integer>> listState;
            //0-29999 -> List()
            //30000-59999 -> List()
            //private transient MapStateState<String, List<Tuple2<String, Integer>>> listState;
            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化或恢复状态
                ListStateDescriptor<Tuple2<String, Integer>> stateDescriptor = new ListStateDescriptor<>("lst-state", TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                }));
                listState = getRuntimeContext().getListState(stateDescriptor);
            }

            @Override
            public void processElement(Tuple2<String, Integer> input, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                long currentWatermark = ctx.timerService().currentWatermark();
                long triggerTime = currentWatermark - currentWatermark % 30000 + 30000;
                if (triggerTime >= 0) {
                    //注册EventTime定时器
                    ctx.timerService().registerEventTimeTimer(triggerTime);
                }
                //将数据添加到ListState中
                listState.add(input);

            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

                ArrayList<Tuple2<String, Integer>> lst = (ArrayList<Tuple2<String, Integer>>) listState.get();

                //先排序
                lst.sort(new Comparator<Tuple2<String, Integer>>() {
                    @Override
                    public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
                        return o2.f1 - o1.f1;
                    }
                });
                //输出top3的数据
                for (int i = 0; i < Math.min(3, lst.size()); i++) {
                    out.collect(lst.get(i));
                }
                //清除该时间的对应的数据
                listState.clear();
            }
        });

        res.print();

        env.execute();


    }


}

