package cn.doitedu.day02;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamFilter;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Filter是对数据进行过滤
 */
public class C07_FilterDemo1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //spark
        //hive
        DataStreamSource<String> words = env.socketTextStream("localhost", 8888);

        //过滤数据，保留以h开头的单词
        //SingleOutputStreamOperator<String> filtered = words.filter(w -> w.startsWith("h"));

        FilterFunction<String> filterFunction = new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return s.startsWith("h");
            }
        };

        SingleOutputStreamOperator<String> filtered = words.transform("MyFilter", TypeInformation.of(String.class), new StreamFilter<>(filterFunction));
        //SingleOutputStreamOperator<String> filtered = words.transform("MyFilter", TypeInformation.of(String.class), new MyStreamFilter());

        filtered.print();

        env.execute();

    }


    public static class MyStreamFilter extends AbstractStreamOperator<String> implements OneInputStreamOperator<String, String> {


        @Override
        public void processElement(StreamRecord<String> element) throws Exception {
            String value = element.getValue();
            if (value.startsWith("h")) {
               output.collect(element);
            }

        }
    }


}
