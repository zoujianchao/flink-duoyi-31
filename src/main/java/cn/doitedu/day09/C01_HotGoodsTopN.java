package cn.doitedu.day09;


import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * 计算一段时间内，热门商品的TopN
 * <p>
 * <p>
 * 时间,用户id,事件类型,商品id,商品分类id
 * 1000,u1001,view,prudoct01,category001
 * 3000,u1002,view,prudoct01,category001
 * 3200,u1002,view,prudoct02,category012
 * 3300,u1002,click,prudoct02,category012
 * 4000,u1002,click,prudoct01,category001
 * 5000,u1003,view,prudoct02,category001
 * 6000,u1004,view,prudoct02,category001
 * 7000,u1005,view,prudoct02,category001
 * 8000,u1005,click,prudoct02,category001
 * <p>
 * <p>
 * 需求：
 * 统计10秒钟内的各种事件、各种商品分类下的热门商品TopN，每2秒出一次结果
 * <p>
 * <p>
 * 思路：划分什么样的窗口（窗口类型，时间类型），数据如何聚合、排序
 * EventTime类型的滑动窗口、增量聚合、排序要将聚合后的数据攒起来（全量）在排序
 */
public class C01_HotGoodsTopN {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        //提取EventTime生成WaterMark
        SingleOutputStreamOperator<String> linesWithWaterMark = lines.assignTimestampsAndWatermarks(WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((line, time) -> Long.parseLong(line.split(",")[0]))
        );

        //对数据进行整理
        SingleOutputStreamOperator<Tuple3<String, String, String>> tpStream = linesWithWaterMark.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[2], fields[3], fields[4]);
            }
        });

        //按照什么字段进行keyBy？ 按照事件类型、和商品分类ID、商品ID

        KeyedStream<Tuple3<String, String, String>, Tuple3<String, String, String>> keyedStream = tpStream.keyBy(new KeySelector<Tuple3<String, String, String>, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> getKey(Tuple3<String, String, String> value) throws Exception {
                return value;
            }
        });

        WindowedStream<Tuple3<String, String, String>, Tuple3<String, String, String>, TimeWindow> windowedStream = keyedStream.window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)));

        //将窗口中的数据进行聚合（增量聚合）
        SingleOutputStreamOperator<ItemEventCount> aggStream = windowedStream.aggregate(new HotGoodsAggFunctions(), new HotGoodsWindowFunctions());

        //还要再进行keyBy，将同一种实现类型，同一个商品分类下所有的商品ID对应的次数，都分到同一个分区中，进行排序
        KeyedStream<ItemEventCount, Tuple2<String, String>> keyedStream2 = aggStream.keyBy(new KeySelector<ItemEventCount, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(ItemEventCount value) throws Exception {
                return Tuple2.of(value.categoryId, value.eventId);
            }
        });

        //使用processFunction + ListState + 定时器
        SingleOutputStreamOperator<ItemEventCount> res = keyedStream2.process(new HotGoodsSortFunction());


    }


    public static class HotGoodsAggFunctions implements AggregateFunction<Tuple3<String, String, String>, Integer, Integer> {

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Tuple3<String, String, String> value, Integer accumulator) {
            return accumulator + 1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return null;
        }
    }


    public static class HotGoodsWindowFunctions implements WindowFunction<Integer, ItemEventCount, Tuple3<String, String, String> ,TimeWindow> {

        @Override
        public void apply(Tuple3<String, String, String> key, TimeWindow window, Iterable<Integer> input, Collector<ItemEventCount> out) throws Exception {
            long start = window.getStart();//窗口起始时间
            long end = window.getEnd(); //窗口结束时间
            Integer count = input.iterator().next();
            String eventId = key.f0;
            String productId = key.f1;
            String categoryId = key.f2;
            out.collect(new ItemEventCount(categoryId, productId, eventId, count, start, end));
        }
    }

    public static class ItemEventCount {

        public String categoryId;  //商品分类ID
        public String productId;     // 商品ID
        public String eventId;     // 事件类型

        public long windowStart;  // 窗口开始时间戳
        public long windowEnd;  // 窗口结束时间戳

        public long count;  // 对应事件的次数量

        public int order;

        public ItemEventCount(String categoryId, String productId, String eventId, long count, long windowStart, long windowEnd) {
            this.productId = productId;
            this.eventId = eventId;
            this.categoryId = categoryId;
            this.count = count;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
        }

        @Override
        public String toString() {
            return "ItemEventCount{" +
                    "categoryId='" + categoryId + '\'' +
                    ", productId='" + productId + '\'' +
                    ", eventId='" + eventId + '\'' +
                    ", count=" + count +
                    ", windowStart=" + windowStart +
                    ", windowEnd=" + windowEnd +
                    ", order=" + order +
                    '}';
        }
    }

    public static class HotGoodsSortFunction extends KeyedProcessFunction<Tuple2<String, String>, ItemEventCount, ItemEventCount> {

        private transient ListState<ItemEventCount> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor<ItemEventCount> stateDescriptor = new ListStateDescriptor<>("list-state", ItemEventCount.class);
            listState = getRuntimeContext().getListState(stateDescriptor);
        }

        @Override
        public void processElement(ItemEventCount value, Context ctx, Collector<ItemEventCount> out) throws Exception {
            listState.add(value);
            //注册定时器
            long windowEnd = value.windowEnd;
            ctx.timerService().registerEventTimeTimer(windowEnd + 1);
        }


        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<ItemEventCount> out) throws Exception {
            ArrayList<ItemEventCount> lst = (ArrayList<ItemEventCount>) listState.get();
            lst.sort(new Comparator<ItemEventCount>() {
                @Override
                public int compare(ItemEventCount o1, ItemEventCount o2) {
                    return Long.compare(o2.count, o1.count);
                }
            });
            for (int i = 0; i < Math.min(3, lst.size()); i++) {
                ItemEventCount itemEventCount = lst.get(i);
                itemEventCount.order = i + 1;
                out.collect(itemEventCount);
            }
            //清空
            listState.clear();
        }
    }

}
