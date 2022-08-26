package cn.doitedu.day02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 将keyBy后的DataStream，按照指定的字段进行聚合运算
 *
 */
public class C15_ReduceDemo1 {

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

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordAndCount.keyBy(t -> t.f0);

        //KeyedStream才可以调用reduce方法
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduced = keyedStream.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> tp1, Tuple2<String, Integer> tp2) throws Exception {
                //对应的次数相加
                tp1.f1 = tp1.f1 + tp2.f1;
                return tp1;
            }
        });

        reduced.print();

        env.execute();


    }

}
