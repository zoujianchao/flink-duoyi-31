package cn.doitedu.day02;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * map，做映射，使用lambda表达式，如果返回的数据类型，有嵌套的泛型，必须在返回的dataStream后面调用returns，指定返回类型
 */
public class C03_MapDemo2 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //spark
        //hive
        DataStreamSource<String> words = env.socketTextStream("localhost", 8888);

        //(spark,1)
        //map返回的数据类型中，包含泛型
        //使用lambda表达式，返回的数据类型中，包含泛型，泛型会被擦除（泛型缺失）
        SingleOutputStreamOperator<Tuple2<String, Integer>> tpStream = words.map(w -> Tuple2.of(w, 1)).returns(Types.TUPLE(Types.STRING, Types.INT));

        tpStream.print();

        env.execute();

    }


}
