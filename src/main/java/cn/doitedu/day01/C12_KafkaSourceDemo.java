package cn.doitedu.day01;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * KafkaSource，并行的Source，用于生产环境，可以保证AtLeastOnceOnce和ExactlyOnce语义
 * 1.在pom中添加依赖 flink-connector-kafka_2.12
 *
 */
public class C12_KafkaSourceDemo {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8082); //指定本地webUI服务的端口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        Properties properties = new Properties();

        //Kafka的Broker地址
        properties.setProperty("bootstrap.servers", "node-1.51doit.cn:9092,node-2.51doit.cn:9092,node-3.51doit.cn:9092");
        //指定消费者组ID
        properties.setProperty("group.id", "test798");
        //如果没有记录历史偏移量就从头读
        properties.setProperty("auto.offset.reset", "earliest");

        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<String>("wordcount", new SimpleStringSchema(), properties);

        DataStreamSource<String> lines = env.addSource(flinkKafkaConsumer);

        System.out.println("KafkaSource的并行度：" + lines.getParallelism());

        lines.print();

        env.execute();


    }
}
