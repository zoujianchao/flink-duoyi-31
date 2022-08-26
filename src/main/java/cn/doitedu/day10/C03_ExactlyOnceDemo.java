package cn.doitedu.day10;

import cn.doitedu.day06.C08_KafkaToRedisWordCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * 如果保证ExactlyOnce
 *
 * Flink支持多种一致性语义，AtLeastOnce 和 ExactlyOnce
 *
 * AtLeastOnce 至少一次，即数据至少被处理一次，但是有可能不重复处理
 * ExactlyOnce 精准一次，即数据被精准处理一次
 *
 * AtLeastOnce前提条件
 * 从Kafka中读取数据，然后写入到Redis中，开启Checkpointing，可以保证AtLeastOnce（Source可以记录偏移量，Sink支持覆盖）
 *
 * ExactlyOnce前提条件
 * 从Kafka中读取数据，然后吸入到Kafka或MySQL中，开启Checkpointing，可以保证ExactlyOnce（Source可以记录偏移量，Sink支持事务）
 *
 * 当前的例子：
 *   从Kafka中读取数据，然后将数据写入到Kafka中
 *
 * 原理
 * FlinkKafkaProducer继承了TwoPhaseCommitSinkFunction抽象类，该类实现了CheckpointedFunction, CheckpointListener
 * ①调用initializeState初始化一个OperatorState（然后将事务相关的信息（transactionId））
 * ②当有数据输入，就会调用invoke方法，将数据调用开启事务的Producer的send方法（缓存到客户端）
 * ③当达到了checkpoint的时间，会调用snapshotState方法，进行preCommit，是将数据flush到Kafka服务端，然后将事务相关的信息（transactionId）保存到OperatorState中
 * ④jobManager集齐了所有subtaks checkpoint成功的ack消息后，会向实现了CheckpointListener的subtask发送rpc消息，让其调用notifyCheckpointComplete方法，然后提交事务
 *
 * 如果提交事务失败，subtask重启，initializeState恢复数据（事务相关的信息（transactionId））提交事务
 *
 *
 */
public class C03_ExactlyOnceDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //开启Checkpoint，默认的CheckpointingMode为EXACTLY_ONCE，如果Source或Sink不支持EXACTLY_ONCE，会降级成AT_LEAST_ONCE
        env.enableCheckpointing(30000, CheckpointingMode.EXACTLY_ONCE);

        //设置flink job的checkpoint对应的StateBackend（状态存储后端）
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(args[0]);

        //设置参数的properties
        Properties properties = new Properties();
        //Kafka的Broker地址
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node-1.51doit.cn:9092,node-2.51doit.cn:9092,node-3.51doit.cn:9092");
        //消费者不自动提交偏移量
        properties.setProperty("enable.auto.commit", "false"); //该参数没有生效
        //如果没有记录历史偏移量就从头读
        properties.setProperty("auto.offset.reset", "earliest");
        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<String>("wc", new SimpleStringSchema(), properties);
        //当flink checkpoint成功后，不自动提交偏移量到Kafka特殊的topic中(__consumer_offsets)
        flinkKafkaConsumer.setCommitOffsetsOnCheckpoints(false);
        DataStreamSource<String> lines = env.addSource(flinkKafkaConsumer);

        SingleOutputStreamOperator<String> res = lines.filter(line -> !line.startsWith("error"));

        //将最终的结果写入到Kafka中
        //往Kafka中写入，要创建Kafka的Producer
        //使用的AtLeastOnce
//        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
//                "kafka-out", //topic名称
//                new SimpleStringSchema(),
//                properties
//        );

        String topic = "kafka-out";

        //事务超时时间改为10分钟
        properties.setProperty("transaction.timeout.ms", "600000"); //broker默认值是为15分钟
        //ExactlyOnce
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                topic,
                new MyKafkaSerializationSchema(topic),
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );

        res.addSink(kafkaProducer);

        env.execute();

    }

}
