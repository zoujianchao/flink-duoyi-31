package cn.doitedu.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class C01_IncomeCount {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //u001,河北省,3000
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        //对数据进行整理
        SingleOutputStreamOperator<Tuple3<String, Double, Integer>> tpStream = lines.map(new MapFunction<String, Tuple3<String, Double, Integer>>() {
            @Override
            public Tuple3<String, Double, Integer> map(String line) throws Exception {
                String[] fields = line.split(",");
                String province = fields[1];
                double money = Double.parseDouble(fields[2]);
                return Tuple3.of(province, money, 1);
            }
        });

        //按照省份进行KeyBy
        KeyedStream<Tuple3<String, Double, Integer>, String> keyedStream = tpStream.keyBy(t -> t.f0);

        //对同一个省份的金额和订单数据进行聚合
        //SingleOutputStreamOperator<Tuple3<String, Double, Integer>> sum1 = keyedStream.sum(1);
        //SingleOutputStreamOperator<Tuple3<String, Double, Integer>> sum2 = keyedStream.sum(2);
        SingleOutputStreamOperator<Tuple3<String, Double, Integer>> provinceMoneyAndOrder = keyedStream.reduce(new ReduceFunction<Tuple3<String, Double, Integer>>() {
            @Override
            public Tuple3<String, Double, Integer> reduce(Tuple3<String, Double, Integer> tp1, Tuple3<String, Double, Integer> tp2) throws Exception {
                //tp1.f1 = tp1.f1 + tp2.f1;
                //tp1.f2 = tp2.f2;
                return tp1;
            }
        });

        provinceMoneyAndOrder.print("省份成交金额和单数：");


        //如果没有预聚合，而是直接按照TOTAL进行聚合，会导致数据倾斜
//        SingleOutputStreamOperator<Tuple3<String, Double, Integer>> total = lines.map(new MapFunction<String, Tuple3<String, Double, Integer>>() {
//            @Override
//            public Tuple3<String, Double, Integer> map(String line) throws Exception {
//                String[] fields = line.split(",");
//
//                //String province = fields[1];
//                double money = Double.parseDouble(fields[2]);
//                return Tuple3.of("TOTAL", money, 1);
//            }
//        });


        //该DataStream已经按照省份进行了预聚合
        KeyedStream<Tuple3<String, Double, Integer>, String> keyedStream2 = provinceMoneyAndOrder.map(new MapFunction<Tuple3<String, Double, Integer>, Tuple3<String, Double, Integer>>() {
            @Override
            public Tuple3<String, Double, Integer> map(Tuple3<String, Double, Integer> tp) throws Exception {
                tp.f0 = "TOTAL";
                return tp;
            }
        }).keyBy(t -> t.f0);

        keyedStream2.reduce(new ReduceFunction<Tuple3<String, Double, Integer>>() {


            @Override
            public Tuple3<String, Double, Integer> reduce(Tuple3<String, Double, Integer> tp1, Tuple3<String, Double, Integer> tp2) throws Exception {
                //TODO 数据被重复累加了
                //tp1.f1 = tp1.f1 + tp2.f1;
                //tp1.f2 = tp1.f2 + tp2.f2;
                return tp1;
            }
        }).print("总的订单金额和单数");


        env.execute();

    }
}
