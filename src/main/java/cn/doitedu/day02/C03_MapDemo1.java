package cn.doitedu.day02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * map，做映射
 */
public class C03_MapDemo1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //spark
        //hive
        DataStreamSource<String> words = env.socketTextStream("localhost", 8888);

        //map方法是并行的算子，默认的并行度与执行环境的并行度一致
//        SingleOutputStreamOperator<String> upper = words.map(new MapFunction<String, String>() {
//            @Override
//            public String map(String s) throws Exception {
//                return s.toUpperCase();
//            }
//        });

        //words.map(w => w.toUpperCase)
        //words.map(_.toUpperCase)
        //SingleOutputStreamOperator<String> upper = words.map(w -> w.toUpperCase());
        SingleOutputStreamOperator<String> upper = words.map(String::toUpperCase);

        upper.print();

        env.execute();

    }


}
