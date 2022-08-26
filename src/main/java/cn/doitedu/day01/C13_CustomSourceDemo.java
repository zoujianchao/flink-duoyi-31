package cn.doitedu.day01;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * 自定义Source，实现了SourceFunction接口的Source的并行度：1，即该Source是非并行的Source
 * 如果run方法没有while循环，即数据读取完，run方法退出，Source就是一个有限的数据流
 */
public class C13_CustomSourceDemo {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8082); //指定本地webUI服务的端口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        DataStreamSource<String> lines = env.addSource(new SourceFunction<String>() {
            //Source启动时调用的方法，该方法用户读取数据，并将数据发送给后面的算子使用
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                System.out.println("run method invoked !!!!");
                List<String> words = Arrays.asList("spsrk", "hive", "flink", "hbase");
                //使用SourceContext将数据输出
                for (String word : words) {
                    //输出
                    ctx.collect(word);
                }

            }

            //将Job Cancel时，会调cancel方法
            @Override
            public void cancel() {
                System.out.println("cancel method invoked @@@@@");
            }
        });

        System.out.println("实现了SourceFunction接口的Source的并行度：" + lines.getParallelism());

        lines.print();

        env.execute();


    }
}
