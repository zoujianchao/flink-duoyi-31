package cn.doitedu.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;


/**
 * Flink的自定义分区
 *
 */
public class C12_CustomPartitionDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //spark,1
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        //将数据进行整理
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndCount = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String line) throws Exception {
                String[] fields = line.split(",");
                return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
            }
        });

        //自定义分区
        DataStream<Tuple2<String, Integer>> partitioned = wordAndCount.partitionCustom(
                new MyCustomPartitioner(),
                t -> t.f0
        );

        partitioned.addSink(
                new RichSinkFunction<Tuple2<String, Integer>>() {
                    @Override
                    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
                        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                        System.out.println(value + " -> " + indexOfThisSubtask);
                    }
                }
        );

        env.execute();


    }

    public static class MyCustomPartitioner implements Partitioner<String> {

        /**
         *
         * @param key 输入的key
         * @param numPartitions 下游分区的数量（并行度）
         * @return
         */
        @Override
        public int partition(String key, int numPartitions) {
            int partition = 0;
            if ("spark".equals(key)) {
                partition = 1;
            } else if ("hadoop".equals(key)) {
                partition = 2;
            } else if ("flink".equals(key)) {
                partition = 3;
            }
            return partition;
        }
    }
}
