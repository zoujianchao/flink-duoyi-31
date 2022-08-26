package cn.doitedu.day02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * FlatMap底层实现
 */
public class C09_FlatMapDemo2 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //spark hive flink
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);


        FlatMapFunction<String, String> flatMapFunction = new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                for (String w : s.split(" ")) {
                    collector.collect(w); //collector内部是使用了Output将数据输出
                }
            }
        };

        //SingleOutputStreamOperator<String> words = lines.transform("MyFlatMap", TypeInformation.of(String.class), new StreamFlatMap<>(flatMapFunction));
        SingleOutputStreamOperator<String> words = lines.transform("MyFlatMap", TypeInformation.of(String.class), new MyStreamFlatMap());


        words.print();


        env.execute();

    }


    public static class MyStreamFlatMap extends AbstractStreamOperator<String> implements OneInputStreamOperator<String, String> {


        @Override
        public void processElement(StreamRecord<String> element) throws Exception {
            String in = element.getValue();
            String[] words = in.split(" ");
            for (String word : words) {
                output.collect(element.replace(word));
            }
        }

    }



}
