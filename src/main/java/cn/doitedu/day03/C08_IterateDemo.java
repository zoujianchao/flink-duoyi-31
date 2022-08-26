package cn.doitedu.day03;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//用来做数据流迭代计算（增强分布式while循环）
public class C08_IterateDemo {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        //10
        DataStreamSource<String> strs = env.socketTextStream("localhost", 8888);

        //将数字转成long类型
        DataStream<Long> numbers = strs.map(Long::parseLong);

        //调用iterate方法 DataStream -> IterativeStream
        //对Nums进行迭代（不停的输入int的数字）
        IterativeStream<Long> iteration = numbers.iterate();

        //IterativeStream -> DataStream
        //对迭代出来的数据进行运算 //对输入的数据应用更新模型，即输入数据的处理逻辑
        DataStream<Long> iterationBody = iteration.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("iterate input =>" + value);
                return value -= 3;
            }
        });

        //只要满足value > 0的条件，就会形成一个回路，重新的迭代，即将前面的输出作为输入，在进行一次应用更新模型，即输入数据的处理逻辑
        //继续迭代的条件
        DataStream<Long> feedback = iterationBody.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value > 0;
            }
        });
        //传入迭代的条件
        iteration.closeWith(feedback);

        //不满足迭代条件的最后要输出
        DataStream<Long> output = iterationBody.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value <= 0;
            }
        });

        //数据结果
        output.print("output value:");

        env.execute();


    }
}

