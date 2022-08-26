package cn.doitedu.day01;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 基于文件的Source，即Source算子生成的Task，以后可以从文件中读取数据
 *
 * readTextFile的执行模式是：FileProcessingMode.PROCESS_ONCE，数据只读取一次，读完就停止（有限数据流）
 *
 */
public class C11_FileSourceDemo1 {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8082); //指定本地webUI服务的端口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        System.out.println("执行环境的的并行度：" + env.getParallelism());

        DataStreamSource<String> lines = env.readTextFile("file:///Users/start/Desktop/words.txt");

        System.out.println("readTextFile 得到的DataStream的并行度：" + lines.getParallelism());

        lines.print();


        env.execute();


    }
}
