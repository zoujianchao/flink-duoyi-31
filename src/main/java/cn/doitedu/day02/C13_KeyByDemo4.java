package cn.doitedu.day02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Tuple类型，KeyBy传入匿名内部类
 *
 */
public class C13_KeyByDemo4 {

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

        //KeySelector中第二个泛型，是key的类型
        KeyedStream<Tuple3<String, String, Integer>, Tuple2<String, String>> keyed = tpStream.keyBy(new KeySelector<Tuple3<String, String, Integer>, Tuple2<String, String>>() {

            @Override
            public Tuple2<String, String> getKey(Tuple3<String, String, Integer> tp) throws Exception {
                return Tuple2.of(tp.f0, tp.f1);
            }
        });


        KeyedStream<Tuple3<String, String, Integer>, String> keyed2 = tpStream.keyBy(new KeySelector<Tuple3<String, String, Integer>, String>() {

            @Override
            public String getKey(Tuple3<String, String, Integer> tp) throws Exception {
                return tp.f0 + tp.f1;
            }
        });

        keyed2.print();


        keyed.print();


        env.execute();


    }

}
