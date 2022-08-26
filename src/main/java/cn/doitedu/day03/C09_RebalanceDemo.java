package cn.doitedu.day03;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * Flink的分区方式有几种
 * 1. keyBy（Hash）
 * 2. rebalance（轮询）
 * 3. rescaling（在一个TaskManager轮询）
 * 4. shuffle (随机)
 * 5. forward (直传)
 * 6. broadcast(广播)
 * 7. partitionCustom (自定义分区)
 *
 *
 *
 * 讲解Flink的物理分区Rebalance（轮询）
 * <p>
 * 上下游并行度不一致，默认数据分区策略就是rebalance
 */
public class C09_RebalanceDemo {

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
        DataStream<String> rebalanced = uppper.rebalance();

        rebalanced.addSink(
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
