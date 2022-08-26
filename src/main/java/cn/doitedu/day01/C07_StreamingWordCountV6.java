package cn.doitedu.day01;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class C07_StreamingWordCountV6 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //调用Source方法创建DataStream
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<String> words = lines.flatMap((String line, Collector<String> collector) -> {
            String[] ws = line.split(" ");
            for (String word : ws) {
                collector.collect(word);
            }
        }).returns(Types.STRING);


        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = words.map(w -> Tuple2.of(w, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT));

        wordAndOne.print();

        //wordAndOne.keyBy(t -> t.f0).sum(1).print();


        //执行
        env.execute();


    }


}
