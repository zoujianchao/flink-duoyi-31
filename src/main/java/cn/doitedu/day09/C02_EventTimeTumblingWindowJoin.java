package cn.doitedu.day09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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

import java.time.Duration;

/**
 * 数据流为什么要进行join关联
 *
 * 向要的数据，来自于两个流（或多个流），而且这个流的数据之间存在着关联关系，将他们关联上
 *
 * 实时的数据如何关联到一起（数据一闪而过，没法关联）
 * 1.两个中的数据必须在相同的地点（让两个中的数据按照相同的条件进行分区）
 * 2.两个中的数据必须在相同时间范围内（划分窗口（WindowJoin）、connect后使用State（IntervalJoin））
 *
 * 该例子演示EventTime类型的滚动窗口的join（InnerJoin）
 */
public class C02_EventTimeTumblingWindowJoin {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1000,o001,已支付
        DataStreamSource<String> lines1 = env.socketTextStream("localhost", 8888);
        //1001,o001,3000,手机
        //1002,o001,2000,家具
        //5000,o002,2000,家具
        DataStreamSource<String> lines2 = env.socketTextStream("localhost", 9999);

        //让两个流进行join
        //o001,已支付,手机,3000
        //o001,已支付,家具,2000

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

        SingleOutputStreamOperator<Tuple2<String, String>> leftStream = lines1WithWaterMark.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[1], fields[2]);
            }
        });

        SingleOutputStreamOperator<Tuple3<String, String, Double>> rightStream = lines2WithWaterMark.map(new MapFunction<String, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[1], fields[3], Double.parseDouble(fields[2]));
            }
        });

        DataStream<Tuple4<String, String, String, Double>> joined = leftStream
                .join(rightStream)
                .where(t1 -> t1.f0) //第一个流的条件
                .equalTo(t2 -> t2.f0) //第二个流的条件
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<Tuple2<String, String>, Tuple3<String, String, Double>, Tuple4<String, String, String, Double>>() {

                    //窗口触发后，在同一个窗口内，并且条件相同
                    @Override
                    public Tuple4<String, String, String, Double> join(Tuple2<String, String> first, Tuple3<String, String, Double> second) throws Exception {
                        return Tuple4.of(first.f0, first.f1, second.f1, second.f2);
                    }
                });

        joined.print();

        env.execute();

    }



}
