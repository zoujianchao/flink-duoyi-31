package cn.doitedu.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * forword叫直传，即上游编号相同的subtaks，传递给下游的subtask，不需要通过网络传输，就是在同一个进程中完成的
 *
 * 要求：上下游并行度一样才能直传
 *
 */
public class C11_ForwardDemo {

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
        }).setParallelism(2).disableChaining(); //强制开启一个新链

        //认为调用forward进行分区（直传）
        //DataStream<String> forward = uppper.forward();

        uppper.addSink(
                new RichSinkFunction<String>() {
                    @Override
                    public void invoke(String value, Context context) throws Exception {
                        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                        System.out.println(value + " -> " + indexOfThisSubtask);
                    }
                }
        ).setParallelism(2);

        env.execute();


    }
}
