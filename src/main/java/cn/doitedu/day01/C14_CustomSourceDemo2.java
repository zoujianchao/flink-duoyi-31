package cn.doitedu.day01;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * 自定义Source，实现了SourceFunction接口的Source的并行度：1，即该Source是非并行的Source
 * 如果run方法有while循环，run方法一直执行，Source就是一个无限的数据流
 */
public class C14_CustomSourceDemo2 {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8082); //指定本地webUI服务的端口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        DataStreamSource<String> lines = env.addSource(new SourceFunction<String>() {

            private boolean flag = true;

            //Source启动时调用的方法，该方法用户读取数据，并将数据发送给后面的算子使用
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                System.out.println("run method invoked !!!!");
                while (flag) {
                    ctx.collect(new Random().nextInt(100) + "");
                    Thread.sleep(1000);
                }
            }

            //将Job Cancel时，会调cancel方法
            @Override
            public void cancel() {
                System.out.println("cancel method invoked @@@@@");
                flag = false;
            }
        });

        System.out.println("实现了SourceFunction接口的Source的并行度：" + lines.getParallelism());

        lines.print();

        env.execute();


    }
}
