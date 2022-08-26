package cn.doitedu.day06;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * KeyedState有三种类型
 *    ValueState<Integer> ： CowMap<KEY, Integer>
 *    MapState<String, Integer> ： CowMap<KEY, HashMap<String, Integer>>
 *    ListState<Integer> ： CowMap<KEY, ArrayList<Integer>>
 *
 *  演示一下ListState
 *
 *    同一个用户，在浏览某个网站，将该用户的最新的10个行为保存起来（按照产生的先后顺序保存）
 *
 */
public class C03_ListStateDemo1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //
        //读取数据
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        /**
         * u001,search
         * u001,view
         * u001,addCart
         * u001,pay
         * u002,view
         * u002,view
         * u002,addCart
         *
         */
        //对数据进行整理
        SingleOutputStreamOperator<Tuple2<String, String>> tpStream = lines.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String line) throws Exception {
                String[] fields = line.split(",");
                return Tuple2.of(fields[0], fields[1]);
            }
        });

        //按照用户ID进行KeyBy
        KeyedStream<Tuple2<String, String>, String> keyedStream = tpStream.keyBy(t -> t.f0);

        SingleOutputStreamOperator<List<String>> res = keyedStream.process(new MyEventListStateFunction());

        res.print();

        env.execute();


    }

    public static class MyEventListStateFunction extends KeyedProcessFunction<String, Tuple2<String, String>, List<String>> {

        private transient ListState<String> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //初始化或恢复状态
            ListStateDescriptor<String> stateDescriptor = new ListStateDescriptor<String>("lst-state", String.class);
            listState = getRuntimeContext().getListState(stateDescriptor);
        }

        @Override
        public void processElement(Tuple2<String, String> value, Context ctx, Collector<List<String>> out) throws Exception {

            String event = value.f1;
            listState.add(event);
            ArrayList<String> events = (ArrayList<String>) listState.get();
            if (events.size() > 10) {
                events.remove(0);
            }

            out.collect(events);

        }
    }



}
