package cn.doitedu.day02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamMap;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * 不直接使用map方法，但是想实现类似Map方法的功能
 *
 * map方法，底层调用的是transform，传入算子的名称，返回的类型，和具体数据处理的实现
 */
public class C05_MapDemo4 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //spark
        DataStreamSource<String> words = env.socketTextStream("localhost", 8888);

        MapFunction<String, String> mapFunction = new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return s.toUpperCase();
            }
        };

        //SPARK
        SingleOutputStreamOperator<String> upperStream = words.transform("MyMap", TypeInformation.of(String.class), new StreamMap<String, String>(mapFunction));


        upperStream.print();

        env.execute();

    }


}
