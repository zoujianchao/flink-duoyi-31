package cn.doitedu.day02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * KeyBy的用法
 *
 * keyBy是按照指的字段的hash进行分区，类似spark的hashPartitioner
 *
 * 可以保证key相同的数据，一定进入到同一个分区中
 *
 * 1.传入下标，但是只能针对于DataStream中的数据类型为TupleN
 *
 */
public class C10_KeyByDemo1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //spark,1
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);


        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndCount = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String line) throws Exception {
                String[] fields = line.split(",");
                return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
            }
        });

        //key的用法：
        //1.传入下标，但是只能针对于DataStream中的数据类型为TupleN
        //keyBy是按照指定key的hash进行分区
        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = wordAndCount.keyBy(0);

        keyed.print();

        env.execute();


    }

}
