package cn.doitedu.day11;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.Properties;

/**
 * 使用Flink跟Kafka整合的新的API
 */
public class C01_FlinkKafkaConsumerNewAPI {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setString("execution.savepoint.path", "file:///Users/start/Documents/dev/flink-31/chk/684d865d69e098b41a26f58bb0aa5629/chk-54");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        //开启checkpoint
        env.enableCheckpointing(15000);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/start/Documents/dev/flink-31/chk");

        String brokerList = "node-1.51doit.cn:9092,node-2.51doit.cn:9092,node-3.51doit.cn:9092";

        KafkaSource<String> source = KafkaSource.<String>builder() //泛型指定是读取出的数据的类型
                //指定Kafka的Broker的地址
                .setBootstrapServers(brokerList)
                //指定读取的topic，可以是一个或多个
                .setTopics("wc")
                //指定的消费者组ID
                .setGroupId("mygroup02") //默认请情况，会在checkpoint是，将Kafka的偏移量保存到Kafka特殊的topic中（__consumer_offsets）
                //消费者读取数据的偏移量的位置
                //从状态中读取以已经提交的偏移量,如果状态装没有，会到Kafka特殊的topic中读取数据
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                //checkpoint成功后，自动提交偏移量到kafka特殊的topic中
                .setProperty("commit.offsets.on.checkpoint","false")
                //指定读取数据的反序列化方式
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> lines = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        Properties properties = new Properties();
        properties.setProperty("transaction.timeout.ms", "600000"); //broker默认值是为15分钟

        //lines.print();
        //使用新的API将数写入到Kafka中
        KafkaSink<String> sink = KafkaSink.<String>builder()
                //指定写入的Kafka的Topic
                .setBootstrapServers(brokerList)
                .setKafkaProducerConfig(properties) //设置Kafka相关参数
                //读取Kafka的topic的相关参数
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("kafka-out") //值的的topic
                        .setValueSerializationSchema(new SimpleStringSchema()) //写入的数据的序列化方式
                        .build()
                )
                //实现的一致性语义
                .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build();


        //不是调用addSink方法，而是调用SinkTo方法
        lines.sinkTo(sink);

        env.execute();

    }

}
