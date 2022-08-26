package cn.doitedu.day06;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *  ListState<String>
 *
 *      CowMap<Key, ArrayList<v>>, 为ListState设置TTL，就是为每一个元素v设置TTL
 *
 */
public class C07_ListStateTTLDemo {

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
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(30)).build();
            ListStateDescriptor<String> stateDescriptor = new ListStateDescriptor<String>("lst-state", String.class);
            //将TTLConfig关联到状态描述器上
            stateDescriptor.enableTimeToLive(ttlConfig);
            listState = getRuntimeContext().getListState(stateDescriptor);
        }

        @Override
        public void processElement(Tuple2<String, String> value, Context ctx, Collector<List<String>> out) throws Exception {

            String event = value.f1;
            listState.add(event);
            Iterable<String> iterable = listState.get();
            ArrayList<String> events = new ArrayList<>();
            //ArrayList<String> events = (ArrayList<String>) listState.get();
            for (String e : iterable) {
                events.add(e);
            }
            out.collect(events);

        }
    }



}
