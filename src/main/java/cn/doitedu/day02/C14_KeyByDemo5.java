package cn.doitedu.day02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * KeyBy使用Lambda表达式
 *
 */
public class C14_KeyByDemo5 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //辽宁省,大连市,2000
        //河北省,唐山市,3000
        //河北省,廊坊市,3000
        //河北省,唐山市,2000
        //辽宁省,大连市,1000
        //辽宁省,沈阳市,1000
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);


        SingleOutputStreamOperator<Tuple3<String, String, Integer>> tpStream = lines.map(new MapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> map(String line) throws Exception {
                String[] fields = line.split(",");
                return Tuple3.of(fields[0], fields[1], Integer.parseInt(fields[2]));
            }
        });

        //将省份相同的数据分到同一个分区
        KeyedStream<Tuple3<String, String, Integer>, String> keyed1 = tpStream.keyBy(tp -> tp.f0);

        //将省份、城市联合起来进行分区
        //keyBy底层new KeyedStream，会使用KeyGroupStreamPartitioner计算分区编号
        KeyedStream<Tuple3<String, String, Integer>, Tuple2<String, String>> keyed2 = tpStream
                .keyBy(
                        tp -> Tuple2.of(tp.f0, tp.f1),
                        Types.TUPLE(Types.STRING, Types.STRING) //key的类型中有泛型，使用lambda表达式会丢失泛型
                );


        keyed2.print();

        env.execute();


    }

}
