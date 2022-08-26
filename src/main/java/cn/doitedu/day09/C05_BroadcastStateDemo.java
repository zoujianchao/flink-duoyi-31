package cn.doitedu.day09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Flink可以特定的状态进行广播（已有的、相对较小，可以实时变化的），实现mapside join
 * 目的是为了实现高效的关联数据
 */
public class C05_BroadcastStateDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //要广播的维度数据
        //c01,手机,INSERT
        //c02,家具,INSERT
        //c03,收音机,INSERT
        //c03,家用电器,UPDATE
        //c03,家用电器,DELETE
        DataStreamSource<String> lines1 = env.socketTextStream("localhost", 8888);

        //o001,c01,3000
        //o002,c02,2000
        //o003,c03,2000
        //o004,c04,2000
        DataStreamSource<String> lines2 = env.socketTextStream("localhost", 9999);

        //关联后的数据
        //o001,c01,3000,手机

        SingleOutputStreamOperator<Tuple3<String, String, Double>> orderStream = lines2.map(new MapFunction<String, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], fields[1], Double.parseDouble(fields[2]));
            }
        });


        SingleOutputStreamOperator<Tuple3<String, String, String>> tpStream = lines1.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], fields[1], fields[2]);
            }
        });

        //使用广播状态进行广播
        //定义状态描述器，即广播后的数据，在下游以何种形式进行存储
        MapStateDescriptor<String, String> stateDescriptor = new MapStateDescriptor<String, String>("cate-state", String.class, String.class);

        //将维度流进行广播
        BroadcastStream<Tuple3<String, String, String>> broadcastStream = tpStream.broadcast(stateDescriptor);

        //希望订单流（事实流）和分类名称流（维度流），发生点关系，即关联
        //即 事实流可以使用维度流中状态
        SingleOutputStreamOperator<Tuple4<String, String, Double, String>> res = orderStream.connect(broadcastStream).process(new BroadcastProcessFunction<Tuple3<String, String, Double>, Tuple3<String, String, String>, Tuple4<String, String, Double, String>>() {

            //处理事实流的数据（订单数据）
            @Override
            public void processElement(Tuple3<String, String, Double> value, ReadOnlyContext ctx, Collector<Tuple4<String, String, Double, String>> out) throws Exception {
                String cid = value.f1;
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(stateDescriptor);
                String name = broadcastState.get(cid);
                out.collect(Tuple4.of(value.f0, value.f1, value.f2, name));
            }

            //处理维度流的数据（分类流）
            @Override
            public void processBroadcastElement(Tuple3<String, String, String> value, Context ctx, Collector<Tuple4<String, String, Double, String>> out) throws Exception {
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(stateDescriptor);
                String cid = value.f0;
                String name = value.f1;
                String type = value.f2;
                System.out.println("subtask :" + getRuntimeContext().getIndexOfThisSubtask() + " ，收到了广播状态：" + value);
                if ("DELETE".equals(type)) {
                    broadcastState.remove(cid);
                } else {
                    broadcastState.put(cid, name);
                }

            }
        });

        res.print();

        env.execute();

    }


}
