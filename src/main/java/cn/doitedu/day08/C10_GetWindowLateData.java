package cn.doitedu.day08;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 使用侧流数据，获取窗口迟到的数据
 *
 * 本质是，就是将迟到的数据打上标签，然后根据指定的标签，获取迟到的数据
 *
 */
public class C10_GetWindowLateData {

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

        OutputTag<Tuple2<String, Integer>> lateDataTag = new OutputTag<Tuple2<String, Integer>>("late-data") {};

        //再划分窗口
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyedStream
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sideOutputLateData(lateDataTag); //将迟到的数据打上标签
        //调用windowOperator
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = windowedStream.sum(1);

        res.print("正常的数据: ");

        //获取迟到数据
        DataStream<Tuple2<String, Integer>> lateDataStream = res.getSideOutput(lateDataTag);

        lateDataStream.print("迟到数据：");

        env.execute();


    }


}
