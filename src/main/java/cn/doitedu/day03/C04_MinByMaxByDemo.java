package cn.doitedu.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Min和MinBy， Max和MaxBy是有区别的
 *
 * 如果就两个字段，一个keyBy的字段，一个比较的字段，min和MinBy，Max和MaxBy每区别
 *
 * Max，如果不但有keyBy的字段，和参与比较的字段，如果还有其他字段，其他字段返回的是同一个组内，第一次出现的
 * MaxBy，如果不但有keyBy的字段，和参与比较的字段，如果还有其他字段，会返回最大值所在的那条数据的全部字段
 *
 */
public class C04_MinByMaxByDemo {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        //spark,1
//        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
//        //将数据进行整理
//        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndCount = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
//            @Override
//            public Tuple2<String, Integer> map(String line) throws Exception {
//                String[] fields = line.split(",");
//                return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
//            }
//        });
//
//        //按照指定的字段进行KeyBy
//        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordAndCount.keyBy(t -> t.f0);
//        //将keyBy后的数据，，将key相同的数据，按照次数，比较大小
//        SingleOutputStreamOperator<Tuple2<String, Integer>> res = keyedStream.maxBy(1);
//
//        res.print();


        //辽宁省,大连市,2000
        //河北省,唐山市,3000
        //河北省,廊坊市,3000
        //河北省,唐山市,2000
        //辽宁省,大连市,1000
        //辽宁省,沈阳市,1000
        //辽宁省,铁岭市,1000
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);


        SingleOutputStreamOperator<Tuple3<String, String, Integer>> tpStream = lines.map(new MapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> map(String line) throws Exception {
                String[] fields = line.split(",");
                return Tuple3.of(fields[0], fields[1], Integer.parseInt(fields[2]));
            }
        });

        //按照省份进行keyBy，即比较同一个省份中的最大成交金额
        KeyedStream<Tuple3<String, String, Integer>, String> keyedStream = tpStream.keyBy(t -> t.f0);

        //如果要比较的字段相等，返回的是前面出现的还是后面出现的
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> res = keyedStream.maxBy(2, false);

        res.print();

        //执行
        env.execute();



    }
}


