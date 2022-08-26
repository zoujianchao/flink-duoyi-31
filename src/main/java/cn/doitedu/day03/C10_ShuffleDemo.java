package cn.doitedu.day03;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * Shuffle是随机分区
 *
 * Flink中将数据按照一定的规律进行分区叫redistribute(类似Spark或MR中的Shuffle概念)
 */
public class C10_ShuffleDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        //将并行设置为2
        SingleOutputStreamOperator<String> uppper = lines.map(new RichMapFunction<String, String>() {
            @Override
            public String map(String str) throws Exception {
                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                return indexOfThisSubtask + " -> " + str;
            }
        }).setParallelism(2);

        //认为调用shuffle进行分区（随机）
        DataStream<String> shuffle = uppper.shuffle();

        shuffle.addSink(
                new RichSinkFunction<String>() {
                    @Override
                    public void invoke(String value, Context context) throws Exception {
                        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                        System.out.println(value + " -> " + indexOfThisSubtask);
                    }
                }
        ).setParallelism(4);

        env.execute();


    }
}
