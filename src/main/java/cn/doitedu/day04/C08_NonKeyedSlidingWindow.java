package cn.doitedu.day04;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 没有KeyBy的滑动窗口（ProcessingTime类型）
 *
 * NonKeyedWindow，Window和WindowOperator所在的DataStream并行度为1
 *
 * SlidingProcessingTimeWindows即ProcessingTIme类型的滚动窗口，会按照系统时间，生成窗口，即使没有数据输入，也会形成窗口
 *
 */
public class C08_NonKeyedSlidingWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //1
        //2
        //3
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //将数据转成integer
        SingleOutputStreamOperator<Integer> nums = lines.map(Integer::parseInt);

        //指定划分窗口的的方式，即按照ProcessingTime划分滑动窗口，需要传入两个时间，第一个是窗口的长度，第二个是滑动的步长
        //AllWindowedStream<Integer, TimeWindow> windowedStream1 = nums.timeWindowAll(Time.seconds(10), Time.seconds(5));

        AllWindowedStream<Integer, TimeWindow> windowedStream = nums.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(20), Time.seconds(10)));

        SingleOutputStreamOperator<Integer> res = windowedStream.sum(0);

        res.print();

        env.execute();

    }
}
