package cn.doitedu.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.HashMap;

/**
 * project：投影（动词）
 * 只能针对DataStream中的数据类Tuple类型使用
 */
public class C07_ProjectDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //辽宁省,大连市,2000
        //河北省,唐山市,3000
        //河北省,廊坊市,3000
        //河北省,唐山市,2000
        //辽宁省,大连市,1000
        //辽宁省,沈阳市,1000
        //辽宁省,铁岭市,1000
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple3<String, String, Integer>> tp3Stream = lines.map(new MapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> map(String line) throws Exception {
                String[] fields = line.split(",");
                return Tuple3.of(fields[0], fields[1], Integer.parseInt(fields[2]));
            }
        });

        //只有java api中有该方法，只能针对于Tuple了类型的Stream
        SingleOutputStreamOperator<Tuple> projected = tp3Stream.project(2, 0);

        //不使用poject，使用map映射也可以实现类似的功能

        projected.print();

        env.execute();


    }

}
