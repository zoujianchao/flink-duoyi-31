package cn.doitedu.day03;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Flink运行算子（map、flatMap、print...）在没有调用redistribute类物理分区方式时（keyBy、shuffle、rebalacne等）
 * 并且两个算子的的并行度一致，默认的策略就是形成一个算子链（OperatorChain），类似Spark中的Pipeline
 * 形成算子链的好处，可以将多个算子在一个subtask中执行，可以节省内存和线程资源
 *
 * startNewChain(); 从该算子的前进开始，开启一个新的连接
 *
 */
public class C14_StartNewChainDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //在Flink的执行环境上调用disableOperatorChaining，禁止全部算子链
        //env.disableOperatorChaining();

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                for (String word : line.split(" ")) {
                    collector.collect(word);
                }
            }
        });

        SingleOutputStreamOperator<String> filter = words.filter(w -> !w.startsWith("error"))
                .startNewChain(); //从该算子的前进开始，开启一个新的连接

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = filter.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return Tuple2.of(word, 1);
            }
        });

        wordAndOne.keyBy(t -> t.f0).sum(1).print();

        env.execute();

    }

}
