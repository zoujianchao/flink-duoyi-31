package cn.doitedu.day03;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.HashMap;

/**
 * 将两个数据流中的数据进行连接（不是join），得到的新的DataStream可以共享原来两个数据流中的状态
 * Connect两个数据流中的数据类型可以不一致
 * 核心功能：共享状态
 * 作业：流的join、流的广播
 */
public class C06_ConnectDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //第一个流（字符串类型）
        DataStreamSource<String> lines1 = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<String> str1 = lines1.map(i -> i.toUpperCase());

        //第二个流（Integer类型）
        DataStreamSource<String> lines2 = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<Integer> nums2 = lines2.map(Integer::parseInt);

        //将两个数据流连到一起，得到的新的数据流，里面依然保存原来两个数据流的类型
        ConnectedStreams<String, Integer> connectedStreams = str1.connect(nums2);

        //调用map，其实是对两个流分别进行map
        SingleOutputStreamOperator<String> res = connectedStreams.map(new CoMapFunction<String, Integer, String>() {

            //定义状态（特殊的存数据的集合）
            //模拟的一个状态
            private HashMap<String, String> myState = new HashMap<>();

            //对第一个流进行map操作
            @Override
            public String map1(String value) throws Exception {
                //可以使用myState
                return value.toUpperCase();
            }

            //对第流个流进行map操作
            @Override
            public String map2(Integer value) throws Exception {
                //可以使用myState
                return value.toString();
            }
        });

        res.print();

        env.execute();


    }

}
