package cn.doitedu.day01;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.LongValueSequenceIterator;

import java.util.Arrays;
import java.util.List;

/**
 * 基于集合，并且是并行的Source，有限数据流，读取完数据后，程序退出
 *
 */
public class C09_ParallelCollectionSourceDemo {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8082); //指定本地webUI服务的端口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        System.out.println("执行环境的的并行度：" + env.getParallelism());

        //Source是多个并行的
        DataStreamSource<LongValue> nums = env.fromParallelCollection(new LongValueSequenceIterator(1, 10), LongValue.class);

        System.out.println("fromParallelCollection 得到的DataStream的并行度：" + nums.getParallelism());

        nums.print();

        env.execute();


    }
}
