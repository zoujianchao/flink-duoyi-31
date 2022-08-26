package cn.doitedu.day02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * 不直接使用map方法，但是想实现类似Map方法的功能
 *
 * map方法，底层调用的是transform，传入算子的名称，返回的类型，和具体数据处理的实现
 *
 * 自定义定义一个StreamMap类
 *
 */
public class C06_MapDemo5 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //spark
        DataStreamSource<String> words = env.socketTextStream("localhost", 8888);

//        MapFunction<String, String> mapFunction = new MapFunction<String, String>() {
//            @Override
//            public String map(String s) throws Exception {
//                return s.toUpperCase();
//            }
//        };

        //SPARK
        SingleOutputStreamOperator<String> upperStream = words.transform("MyMap", TypeInformation.of(String.class), new MyStreamMap());


        upperStream.print();

        env.execute();

    }


    public static class MyStreamMap extends AbstractStreamOperator<String> implements OneInputStreamOperator<String, String> {


        //该算子对应的小task，每处理一条数据，就调用一次processElement方法

        /**
         *
         * @param element 将输入的数据封装到StreamRecord中
         * @throws Exception
         */
        @Override
        public void processElement(StreamRecord<String> element) throws Exception {
            String input = element.getValue();
            String upper = input.toUpperCase();
            //将数据输出
            //output.collect(new StreamRecord<>(upper));
            output.collect(element.replace(upper));
        }
    }

}
