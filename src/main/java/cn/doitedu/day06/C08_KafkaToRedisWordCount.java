package cn.doitedu.day06;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * 从Kafka中读取数据，并且将聚合后的结果保存到Redis中
 * <p>
 * 如果程序出现异常（可以容错），保证计算的结果最终是正确的（AtLeastOnce）
 * <p>
 * 如何容错：
 * 状态（偏移量、累加的单词和次数）
 * 开启Checkpoint
 */
public class C08_KafkaToRedisWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //开启Checkpoint
        env.enableCheckpointing(10000);

        //设置flink job的checkpoint对应的StateBackend（状态存储后端）
        env.setStateBackend(new FsStateBackend(args[0]));
        //将job在web页面cancel后，不删除最近的checkpoint数据
        //CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION 默认的，即job cancel后，外部的checkpoint数据就删除了
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //设置参数的properties
        Properties properties = new Properties();
        //Kafka的Broker地址
        properties.setProperty("bootstrap.servers", "node-1.51doit.cn:9092,node-2.51doit.cn:9092,node-3.51doit.cn:9092");
        //指定消费者组ID
        //properties.setProperty("group.id", "test798");
        //消费者不自动提交偏移量
        properties.setProperty("enable.auto.commit", "false"); //该参数没有生效
        //如果没有记录历史偏移量就从头读
        properties.setProperty("auto.offset.reset", "earliest");
        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<String>("wc", new SimpleStringSchema(), properties);
        //当flink checkpoint成功后，不自动提交偏移量到Kafka特殊的topic中(__consumer_offsets)
        flinkKafkaConsumer.setCommitOffsetsOnCheckpoints(false);
        DataStreamSource<String> lines = env.addSource(flinkKafkaConsumer);


        SingleOutputStreamOperator<Tuple2<String, Integer>> res = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                //即将一行多个单词进行切分，又将单词和1组合
                for (String word : line.split(" ")) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        }).keyBy(t -> t.f0)
                .sum(1);

        //设置Jedis的相关参数，并且创建Jedis连接池
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost("node-1.51doit.cn")
                .setPort(6379)
                //.setPassword("123456")
                .setDatabase(8)
                .build();

        //将数据写入到Redis中
        res.addSink(new RedisSink<Tuple2<String, Integer>>(conf, new RedisWordCountMapper()));

        env.execute();

    }

    //将Flink产生的数据，写入到Redis的映射（就是将数据以何种方式，哪个作为key，哪个作为value）
    public static class RedisWordCountMapper implements RedisMapper<Tuple2<String, Integer>> {

        //获取Redis命令的类型(写入的方式)，已经大KEY的名称
        @Override
        public RedisCommandDescription getCommandDescription() {
            //Map(WORD_COUNT, ((spark,1), (hive,5)))
            return new RedisCommandDescription(RedisCommand.HSET, "WORD_COUNT");
        }

        //将数据中的哪个字段取出来作为里面的小key
        @Override
        public String getKeyFromData(Tuple2<String, Integer> data) {
            return data.f0;
        }
        //将数据中的哪个字段取出来作为里面的小value
        @Override
        public String getValueFromData(Tuple2<String, Integer> data) {
            return data.f1.toString();
        }
    }


}
