package cn.doitedu.day03;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 将Key相同的，滚动的比较指定的字段的大小，min返回最新的，max返回最大的
 */
public class C03_MinMaxDemo {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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

        //按照指定的字段进行KeyBy
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordAndCount.keyBy(t -> t.f0);
        //将keyBy后的数据，，将key相同的数据，按照次数，比较大小
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = keyedStream.max(1);

        res.print();

        //执行
        env.execute();



    }
}


