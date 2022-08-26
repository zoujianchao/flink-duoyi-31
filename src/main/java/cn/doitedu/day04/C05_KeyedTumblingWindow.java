package cn.doitedu.day04;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 先KeyBy，在按照ProcessingTime划分的滚动窗口
 *
 * KeyedWindow，Window和WindowOperator所在的DataStream并行度可以为多个
 *
 * TumblingProcessingTimeWindows即ProcessingTIme类型的滚动窗口，会按照系统时间，生成窗口，即使没有数据输入，也会形成窗口
 *
 * 如果调用sum、reduce方法，在窗口内进行增量聚合，触发后将增量聚合的结果再输出
 *
 * 当窗口触发后，每个分区中的每一个组，都会触发，输出结果
 *
 */
public class C05_KeyedTumblingWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //spark,3
        //hive,2
        //flink,1
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> tpStream = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String line) throws Exception {
                String[] fields = line.split(",");
                return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
            }
        });

        //先keyBy再按照Processing划分滚动窗口
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = tpStream.keyBy(t -> t.f0);
        //window方法中传入WindowAssigner（划分窗口的方式）
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<Tuple2<String, Integer>> res = windowedStream.sum(1);

        res.print();

        env.execute();

    }
}
