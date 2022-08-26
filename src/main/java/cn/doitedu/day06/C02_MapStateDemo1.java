package cn.doitedu.day06;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * KeyedState有三种类型
 * ValueState<Integer> ： CowMap<KEY, Integer>
 * MapState<String, Integer> ： CowMap<KEY, HashMap<String, Integer>>
 * ListState<Integer> ： CowMap<KEY, ArrayList<Integer>>
 * <p>
 * 现在演示MapState
 * <p>
 * 河北省,廊坊市,3000
 * 河北省,唐山市,2000
 * 河北省,廊坊市,3000
 * 河北省,唐山市,1000
 * 山东省,济南市,4000
 * 山东省,烟台市,2000
 * 山东省,济南市,4000
 * <p>
 * 按照省份进行KeyBy，但是在同一个分区内，将相同城市的金额进行累加
 */
public class C02_MapStateDemo1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取数据
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        /**
         * 河北省,廊坊市,3000
         * 河北省,唐山市,2000
         * 河北省,廊坊市,3000
         * 河北省,唐山市,1000
         * 山东省,济南市,4000
         * 山东省,烟台市,2000
         * 山东省,济南市,4000
         */
        //对数据进行整理
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> tpStream = lines.map(new MapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> map(String line) throws Exception {
                String[] fields = line.split(",");
                return Tuple3.of(fields[0], fields[1], Integer.parseInt(fields[2]));
            }
        });

        //按照省份进行keyBy
        KeyedStream<Tuple3<String, String, Integer>, String> keyedStream = tpStream.keyBy(t -> t.f0);

        //自定义UDF
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> res = keyedStream.map(new MyMapStateFunction());

        res.print();

        env.execute();


    }


    public static class MyMapStateFunction extends RichMapFunction<Tuple3<String, String, Integer>, Tuple3<String, String, Integer>> {

        private transient MapState<String, Integer> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //初始化或恢复状态
            MapStateDescriptor<String, Integer> stateDescriptor = new MapStateDescriptor<String, Integer>("map-state", String.class, Integer.class);
            mapState = getRuntimeContext().getMapState(stateDescriptor);
        }

        @Override
        public Tuple3<String, String, Integer> map(Tuple3<String, String, Integer> value) throws Exception {

            String city = value.f1;

            //传入的小key（即城市）
            Integer money = mapState.get(city);
            if(money == null) {
                money = 0;
            }
            money += value.f2;
            //更新
            mapState.put(city, money);

            return Tuple3.of(value.f0, city, money);
        }
    }

}
