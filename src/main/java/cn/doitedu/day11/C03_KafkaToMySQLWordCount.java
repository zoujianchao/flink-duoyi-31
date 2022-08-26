package cn.doitedu.day11;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 从Kafka中读取数据，然后将数据进行keyBy聚合，再将结果写入到MySQL
 *
 */
public class C03_KafkaToMySQLWordCount {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        //conf.setString("execution.savepoint.path", "file:///Users/start/Documents/dev/flink-31/chk/684d865d69e098b41a26f58bb0aa5629/chk-54");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        //开启checkpoint
        env.enableCheckpointing(60000);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(args[0]);

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

        //将处理后数据写入到MySQL
        //1,tom,18
        DataStreamSource<String> lines = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        SingleOutputStreamOperator<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] fields = value.split(" ");
                for (String word : fields) {
                    out.collect(word);
                }
            }
        }).disableChaining()//将该算子前后的链都断开
                .slotSharingGroup("doit"); //设置当月算子对应subtask所在槽的名称，flink默认情况，槽的名称为default
        //一旦对某个算子修改槽的名称，那么该算子以后对应的subtask的槽的名称也会改变（就近跟随原则）
        //flink的只允许资源槽标签（组名称）一致的可以共享同一个资源槽

        SingleOutputStreamOperator<Tuple2<String, Integer>> worAndOne = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        }).slotSharingGroup("default");

        SingleOutputStreamOperator<Tuple2<String, Integer>> res = worAndOne.keyBy(t -> t.f0).sum(1);

        String sql = "insert into tb_wordcount (word, counts) values (?, ?) ON DUPLICATE KEY UPDATE counts = ? ";
        //使用JDBC Sink（第一种，实现AtLeastOnce）
        SinkFunction<Tuple2<String, Integer>> mysqlSink = JdbcSink.sink(
                sql, //要执行的SQL
                //将参数和preparedStatement进行映射
                new JdbcStatementBuilder<Tuple2<String, Integer>>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, Tuple2<String, Integer> tp) throws SQLException {
                        //将参数和preparedStatement对应的位置进行绑定
                        preparedStatement.setString(1, tp.f0); //word
                        preparedStatement.setInt(2, tp.f1); //5
                        preparedStatement.setInt(3, tp.f1); //5
                    }
                },
                //设置执行相关的参数
                JdbcExecutionOptions.builder()
                        .withBatchSize(100) //每次每个subtask中的数据达到多少天写入一次
                        .withBatchIntervalMs(1000) //当与上次一次写入到时间超过1秒写入一次，两个条件满足任何其一，就会写入
                        .withMaxRetries(5) //写入失败最大的重试次数
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://node-1.51doit.cn:3306/doit31?characterEncoding=utf-8")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()
        );

        res.addSink(mysqlSink);

        env.execute();

    }

}
