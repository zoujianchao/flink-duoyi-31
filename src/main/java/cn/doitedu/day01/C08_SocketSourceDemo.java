package cn.doitedu.day01;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 演示Flink流计算的SocketSource，指定以后生成的Task，从指定的地址和端口读取数据
 */
public class C08_SocketSourceDemo {

    public static void main(String[] args) throws Exception {

        //创建一个本地执行的环境（只能以local模式运行，没法提交到集群中执行），并且有webUI服务
        //如果想在本地模式引入WebUI，需要在pom.xml中添加以下依赖
        //flink-runtime-web_2.12
        Configuration configuration = new Configuration();
        //执行环境的并行度（local模式运行，你的计算机CPU的线程数）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        System.out.println("执行环境的的并行度：" + env.getParallelism());

        //Source分为并行的Source（Source算子对应的Task是多个），和非并行的Source（Source算子对应的Task只有一个）
        //使用SocketSource，非并行的Source，即使用一个Task读取数据，该Source很少用于生产环境，通常用户测试和学习使用
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        int parallelism = lines.getParallelism();
        System.out.println("Socket Source的并行度：" + parallelism);

        DataStreamSink<String> streamSink = lines.print();

        System.out.println("print Sink的并行度：" + streamSink.getTransformation().getParallelism());

        env.execute();


    }
}
