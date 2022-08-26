package cn.doitedu.day04;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 生成WaterMark所在的DataStream的并行度如果大于1，即有多个subtask生成WaterMark（向下游发送特殊的信号）
 *
 *
 */
public class C15_EventTimeKeyedTumblingWindowWithMultiPartition {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //1000,spark,1
        //socketTextStream得到的DataStream并行度永远是1，以后可以使用Kafka（多并行的Source）
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //tpStream并行度数多个并行，与env的并行度保存一致
        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> tpStream = lines.map(new MapFunction<String, Tuple3<Long, String, Integer>>() {
            @Override
            public Tuple3<Long, String, Integer> map(String line) throws Exception {
                String[] fields = line.split(",");
                long eventTime = Long.parseLong(fields[0]);
                String word = fields[1];
                int count = Integer.parseInt(fields[2]);
                return Tuple3.of(eventTime, word, count);
            }
        });
        //对tpStream提取EventTime生成WaterMark,
        //调用assignTimestampsAndWatermarks方法得到的DataStream的并行度，与调用该方法的DataStream并行度保持一致
        //设置延迟时间为0
        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> tpStreamWithWaterMark = tpStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Long, String, Integer>>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(Tuple3<Long, String, Integer> tp) {
                return tp.f0;
            }
        });
        //按照单词进行keyBY
        KeyedStream<Tuple3<Long, String, Integer>, String> keyedStream = tpStreamWithWaterMark.keyBy(t -> t.f1);

        //划分窗口
        WindowedStream<Tuple3<Long, String, Integer>, String, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));

        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> res = windowedStream.sum(2);

        res.print();

        env.execute();
    }

}
