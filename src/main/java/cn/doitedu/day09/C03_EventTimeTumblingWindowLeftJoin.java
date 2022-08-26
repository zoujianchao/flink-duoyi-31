package cn.doitedu.day09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 数据流为什么要进行join关联
 * <p>
 * 向要的数据，来自于两个流（或多个流），而且这个流的数据之间存在着关联关系，将他们关联上
 * <p>
 * 实时的数据如何关联到一起（数据一闪而过，没法关联）
 * 1.两个中的数据必须在相同的地点（让两个中的数据按照相同的条件进行分区）
 * 2.两个中的数据必须在相同时间范围内（划分窗口（WindowJoin）、connect后使用State（IntervalJoin））
 * <p>
 * 该例子演示EventTime类型的滚动窗口的join(LeftOuterJoin)
 *
 * join的底层实现
 *  1.将两个流分别使用同样的包装类进行包装（TaggedUnion<T1, T2>）
 *  2.将两个包装后的流，union到一起
 *  3.将union的流，分别传入对应的keyBy条件（条件相同的数据一定会分到同一个分区中）
 *  4.将流划分窗口（将数据缓存到windowState中）
 *  5.窗口触发，对窗口内相同key的数据进行处理（都调用cogroup）
 *
 */
public class C03_EventTimeTumblingWindowLeftJoin {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        //1000,o001,已支付
        //5000,o002,已支付
        DataStreamSource<String> lines1 = env.socketTextStream("localhost", 9999);

        //1001,o001,3000,手机
        //1002,o001,2000,家具
        //2002,o002,2000,家具
        //5000,o002,2000,家具
        DataStreamSource<String> lines2 = env.socketTextStream("localhost", 8888);

        //将订单明细表当成左表，订单主表当成右表
        //让两个流进行LeftOutjoin

        SingleOutputStreamOperator<String> lines1WithWaterMark = lines1.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String element, long recordTimestamp) {
                return Long.parseLong(element.split(",")[0]);
            }
        }));

        SingleOutputStreamOperator<String> lines2WithWaterMark = lines2.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String element, long recordTimestamp) {
                return Long.parseLong(element.split(",")[0]);
            }
        }));

        SingleOutputStreamOperator<Tuple2<String, String>> rightStream = lines1WithWaterMark.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[1], fields[2]);
            }
        });

        SingleOutputStreamOperator<Tuple3<String, String, Double>> leftStream = lines2WithWaterMark.map(new MapFunction<String, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[1], fields[3], Double.parseDouble(fields[2]));
            }
        });

        //让两个流事先左外连接（leftStream即使没有join上，也要显式出来）
        DataStream<Tuple4<String, String, Double, String>> res = leftStream.coGroup(rightStream) //coGroup是对两个流进行协同分组
                .where(t1 -> t1.f0)
                .equalTo(t2 -> t2.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new CoGroupFunction<Tuple3<String, String, Double>, Tuple2<String, String>, Tuple4<String, String, Double, String>>() {

                    /**
                     * 窗口触发后，该key必须在窗口内的至少在一个流中出现，就会调用coGroup
                     * 例如：
                     * 1（innerJoin） 一个key在左流中出现了2次，右边流出现了1次，那么first这个集合中的size为2， second这个集合中的size为1
                     * 2（LeftJoin）  一个key在左流中出现了2次，右边流出现了0次，那么first这个集合中的size为2， second这个集合中的size为0
                     * 3（RightJoin） 一个key在左流中出现了0次，右边流出现了1次，那么first这个集合中的size为0， second这个集合中的size为1
                     * @param first 来自左流的数据
                     * @param second 来自右流的数据
                     * @param out
                     * @throws Exception
                     */
                    @Override
                    public void coGroup(Iterable<Tuple3<String, String, Double>> first, Iterable<Tuple2<String, String>> second, Collector<Tuple4<String, String, Double, String>> out) throws Exception {
                        boolean isJoined = false;
                        //循环左流中的数据，如果for循环中的逻辑执行了，那么说明该key在第一流有数据
                        for (Tuple3<String, String, Double> left : first) {
                            for (Tuple2<String, String> right : second) {
                                isJoined = true;
                                out.collect(Tuple4.of(left.f0, left.f1, left.f2, right.f1));
                            }
                            if (!isJoined) {
                                out.collect(Tuple4.of(left.f0, left.f1, left.f2, null));
                            }
                        }

                    }
                });

        res.print();

        env.execute();

    }


}
