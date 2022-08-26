package cn.doitedu.day04;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 按照EventTime划分滚动窗口（没有keyBy）
 */
public class C12_EventTimeNonKeyedTumblingWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //env.getConfig().setAutoWatermarkInterval(200); 设置发送WaterMark间隔时间

        //WaterMark = 每个分区中最大的EventTime - 延迟时间
        //窗口触发时时机：WaterMark >= 窗口的结束边界
        //1655970600000,1  输入该数据，就生成了一个窗口[1655970600000, 1655970605000) 或 [1655970600000, 1655970604999]
        //1655970602000,2
        //1655970601000,3
        //1655970604000,4
        //1655970604998,4
        //1655970603666,4
        //1655970604999,4
        //1655970605000,5 输入该数据，就生成了一个窗口[1655970605000, 1655970610000) 或 [1655970605000, 1655970609999]
        //1655970606000,5
        //1655970609998,5
        //1655970610000,5 输入该数据，就生成了一个窗口[1655970610000, 1655970615000) 或 [1655970610000, 1655970614999]
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //提取数据中的时间，生成WaterMark（特殊写信号，触发EventTime类型窗口的统一标准）
        //linesWithWaterMark不但对应的数据，还带有特殊的信号（WaterMark）
        //调用完该方法，不会该表原来数据的样子，仅是多了WaterMark
        SingleOutputStreamOperator<String> linesWithWaterMark = lines.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) { //数据乱序延迟触发的时间
            @Override
            public long extractTimestamp(String element) {
                String[] fields = element.split(",");
                return Long.parseLong(fields[0]);
            }
        });

        SingleOutputStreamOperator<Integer> nums = linesWithWaterMark.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String line) throws Exception {
                String[] fields = line.split(",");
                return Integer.parseInt(fields[1]);
            }
        });

        //不KeyBY，划分滚动窗口
        AllWindowedStream<Integer, TimeWindow> windowedStream = nums.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)));

        SingleOutputStreamOperator<Integer> res = windowedStream.sum(0);

        res.print();

        env.execute();
    }

}
