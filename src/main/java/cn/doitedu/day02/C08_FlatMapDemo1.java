package cn.doitedu.day02;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamFilter;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * FlatMap ,对DataStream中的数据进行扁平化映射
 *
 * 如果使用flatMap，方法中传入了lambda表达式，方法调用完，一定要调用returns，指定返回的数据类型
 * 因为flatMap方法输入一条数据，会返回多条数据，返回的多样数据被封装到Collector<T>, Collector中有泛型
 */
public class C08_FlatMapDemo1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //spark hive flink
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

//        SingleOutputStreamOperator<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
//
//            @Override
//            public void flatMap(String in, Collector<String> collector) throws Exception {
//                for (String w : in.split(" ")) {
//                    collector.collect(w);
//                }
//            }
//        });


        SingleOutputStreamOperator<String> words = lines
                .flatMap((String line, Collector<String> collector) -> Arrays.stream(line.split(" ")).forEach(collector::collect))
                .returns(TypeInformation.of(String.class));

        words.print();


        env.execute();

    }





}
