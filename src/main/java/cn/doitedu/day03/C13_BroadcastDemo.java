package cn.doitedu.day03;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * 广播分区，上游一个subtask产生的数据，会广播给下游的每一个subtaks
 */
public class C13_BroadcastDemo {

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

        //认为调用rebalance进行分区
        DataStream<String> broadcast = uppper.broadcast();

        broadcast.addSink(
                new RichSinkFunction<String>() {
                    @Override
                    public void invoke(String value, Context context) throws Exception {
                        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                        System.out.println(value + " -> " + indexOfThisSubtask);
                    }
                }
        );

        env.execute();


    }
}
