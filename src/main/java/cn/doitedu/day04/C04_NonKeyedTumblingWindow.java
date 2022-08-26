package cn.doitedu.day04;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 没有KeyBy的滚动窗口（ProcessingTime类型）
 *
 * NonKeyedWindow，Window和WindowOperator所在的DataStream并行度为1
 *
 * TumblingProcessingTimeWindows即ProcessingTIme类型的滚动窗口，会按照系统时间，生成窗口，即使没有数据输入，也会形成窗口
 *
 */
public class C04_NonKeyedTumblingWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //1
        //2
        //3
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //将数据转成integer
        SingleOutputStreamOperator<Integer> nums = lines.map(Integer::parseInt);

        //指定划分窗口的的方式，即按照ProcessingTime划分滚动窗口
        //AllWindowedStream<Integer, TimeWindow> windowedStream1 = nums.timeWindowAll(Time.seconds(10));
        //WindowAssigner窗口划分器，指定划分窗口的方式（按照什么类型的时间，划分什么样类型的窗口）
        AllWindowedStream<Integer, TimeWindow> windowedStream = nums.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<Integer> res = windowedStream.sum(0);

        res.print();

        env.execute();

    }
}
