package cn.doitedu.day06;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class C04_IncomeCountV2 {

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
        SingleOutputStreamOperator<Tuple3<String, Double, Integer>> provinceMoneyAndOrder = keyedStream.reduce(new ReduceFunction<Tuple3<String, Double, Integer>>() {
            @Override
            public Tuple3<String, Double, Integer> reduce(Tuple3<String, Double, Integer> tp1, Tuple3<String, Double, Integer> tp2) throws Exception {
                tp1.f1 = tp1.f1 + tp2.f1;
                tp1.f2 = tp1.f2 + tp2.f2;
                return tp1;
            }
        });

        provinceMoneyAndOrder.print("省份成交金额和单数：");


        //如果没有预聚合，而是直接按照TOTAL进行聚合，会导致数据倾斜
        SingleOutputStreamOperator<Tuple4<String, String, Double, Integer>> total = provinceMoneyAndOrder.map(new MapFunction<Tuple3<String, Double, Integer>, Tuple4<String, String, Double, Integer>>() {
            @Override
            public Tuple4<String, String, Double, Integer> map(Tuple3<String, Double, Integer> input) throws Exception {

                String province = input.f0;
                Double money = input.f1;
                Integer count = input.f2;
                return Tuple4.of("TOTAL", province , money, count);
            }
        });

        KeyedStream<Tuple4<String, String, Double, Integer>, String> keyedStream2 = total.keyBy(t -> t.f0);

        SingleOutputStreamOperator<Tuple2<Double, Integer>> res = keyedStream2.process(new KeyedProcessFunction<String, Tuple4<String, String, Double, Integer>, Tuple2<Double, Integer>>() {

            private MapState<String, Tuple2<Double, Integer>> mapState;

            @Override
            public void open(Configuration parameters) throws Exception {
                MapStateDescriptor<String, Tuple2<Double, Integer>> stateDescriptor = new MapStateDescriptor<>("m-state", TypeInformation.of(String.class), TypeInformation.of(new TypeHint<Tuple2<Double, Integer>>() {
                }));
                mapState = getRuntimeContext().getMapState(stateDescriptor);
            }

            @Override
            public void processElement(Tuple4<String, String, Double, Integer> input, Context ctx, Collector<Tuple2<Double, Integer>> out) throws Exception {
                String province = input.f1;
                Double money = input.f2;
                Integer count = input.f3;
                mapState.put(province, Tuple2.of(money, count));

                Iterable<Tuple2<Double, Integer>> moneyAndCount = mapState.values();
                double totalMoney = 0.0;
                int totalCount = 0;
                for (Tuple2<Double, Integer> tp : moneyAndCount) {
                    totalMoney += tp.f0;
                    totalCount += tp.f1;
                }
                out.collect(Tuple2.of(totalMoney, totalCount));
            }
        }).setParallelism(1);

        res.print().setParallelism(1);


        env.execute();

    }
}
