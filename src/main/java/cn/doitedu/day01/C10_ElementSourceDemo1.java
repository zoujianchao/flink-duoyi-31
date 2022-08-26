package cn.doitedu.day01;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * 基于集合的Source，就是从一个集合中，读取数据，生成的DataStream使用有限数据流，数据处理完后，程序退出
 * 并且fromCollection这个Source，生成的DataStream的并行度为1，即该Source方式是非并行的Source，用于做测试和实验的
 */
public class C10_ElementSourceDemo1 {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8082); //指定本地webUI服务的端口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        System.out.println("执行环境的的并行度：" + env.getParallelism());

        //基于集合的Soruce
        DataStreamSource<Integer> nums = env.fromElements(1,2,3,4,5,6,7,8,9);

        System.out.println("fromElements 得到的DataStream的并行度：" + nums.getParallelism());

        SingleOutputStreamOperator<Integer> mapStream = nums.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer i) throws Exception {
                return i * 10;
            }
        });

        System.out.println("经过map后，返回的DataStream的并行度为：" + mapStream.getParallelism());

        mapStream.print();

        env.execute();


    }
}
