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
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * IntervalJoin是按照时间范围进行join，即指定第一个流中的数据，向前或向后可以join指定的时间范围
 *
 * 1.将两个数据分别流按照相同的条件进行keyBy(将条件相同的数据分到同一个分区中)
 * 2.然后再将两个流进行connect（使用共享的状态），将两个流的数据保存到共享状态中（KeyedState中的MapState），进行连接
 * 3.然后比较时间是否在指定的时间范围内，如果在同一范围内，就输出数据
 * 4.分别为两个KeyedState注册定时器，如果超过一定时间范围的数据，就会别从状态中移除
 *
 */
public class C04_EventTimeIntervalJoin {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1000,o001,已支付
        DataStreamSource<String> lines1 = env.socketTextStream("localhost", 8888);


        //0,o001,3000,手机
        //1999,o001,2000,家具
        //2000,o001,2000,家具
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

        //对左边的流进行keyBy
        SingleOutputStreamOperator<Tuple4<String, String, String, Double>> res = leftStream.keyBy(t -> t.f0)
                .intervalJoin(rightStream.keyBy(t -> t.f0))
                .between(Time.seconds(-1), Time.seconds(1))
                .upperBoundExclusive() //不包含后面的[,)
                .process(new ProcessJoinFunction<Tuple2<String, String>, Tuple3<String, String, Double>, Tuple4<String, String, String, Double>>() {
                    /**
                     * 数据的条件相同（key相同），并且数据在一定的范围内
                     * @param left
                     * @param right
                     * @param ctx
                     * @param out
                     * @throws Exception
                     */
                    @Override
                    public void processElement(Tuple2<String, String> left, Tuple3<String, String, Double> right, Context ctx, Collector<Tuple4<String, String, String, Double>> out) throws Exception {

                        out.collect(Tuple4.of(left.f0, left.f1, right.f1, right.f2));

                    }
                });

        res.print();

        env.execute();

    }



}
