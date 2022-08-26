package cn.doitedu.day06;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *
 * 公告公司，广告的曝光、点击
 * 实时通过广告的曝光（view）、点击（click）的次数和人数
 *
 * 广告id,事件类型,用户id或设备id
 *
 * a001,view,u001
 * a001,view,u001
 * a001,click,u001
 * a001,view,u002
 * a001,click,u002
 * a002,view,u003
 * a002,view,u003
 * a002,view,u003
 * a002,click,u003
 *
 *
 * 统计各个广告、各种事件类型，对应的人数和次数
 *
 */
public class C01_AdCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //为了容错，必须开启checkpoint
        env.enableCheckpointing(5000);

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple3<String, String, String>> tpStream = lines.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                String aid = fields[0];
                String event = fields[1];
                String uid = fields[2];
                return Tuple3.of(aid, event, uid);
            }
        });

        //按照广告ID和事件类型联合起来进行keyBy
        KeyedStream<Tuple3<String, String, String>, Tuple2<String, String>> keyedStream = tpStream.keyBy(t -> Tuple2.of(t.f0, t.f1), Types.TUPLE(Types.STRING, Types.STRING));

        //自定义Function,并且在UDF使用状态
        SingleOutputStreamOperator<Tuple4<String, String, Long, Long>> res = keyedStream.process(new C01_AdCountFunction()).name("my-udf");

        res.print();

        env.execute();


    }


}
