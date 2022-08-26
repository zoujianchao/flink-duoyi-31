package cn.doitedu.day03;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 将两个或多个数据类型相同的DataStream进行合并，以后可以进行统一操作
 */
public class C05_UnionDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStreamSource<String> lines1 = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Integer> nums1 = lines1.map(i -> Integer.parseInt(i))
                .setParallelism(3);

        DataStreamSource<String> lines2 = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Integer> nums2 = lines2.map(Integer::parseInt);
                //.setParallelism(2);


        DataStream<Integer> union = nums1.union(nums2);

        union.print();

        env.execute();

    }
}
