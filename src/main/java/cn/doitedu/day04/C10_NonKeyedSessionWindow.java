package cn.doitedu.day04;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 没有KeyBy的会话窗口（ProcessingTime类型）
 *
 * 当前系统时间 - 上一条进入到窗户中的数据对应的使用 > 字段的时间间隔，窗口触发
 *
 * NonKeyedWindow，Window和WindowOperator所在的DataStream并行度为1
 *
 * ProcessingTimeSessionWindows即ProcessingTIme类型的回话窗口
 *
 */
public class C10_NonKeyedSessionWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //1
        //2
        //3
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //将数据转成integer
        SingleOutputStreamOperator<Integer> nums = lines.map(Integer::parseInt);

        AllWindowedStream<Integer, TimeWindow> windowedStream = nums.windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(10)));

        SingleOutputStreamOperator<Integer> res = windowedStream.sum(0);

        res.print();

        env.execute();

    }
}
