package cn.doitedu.day11;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
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
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * jdbc sink的使用，即将FLink 处理后的数据写入到MySQL中
 */
public class C02_JDBCSinkDemo1 {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        //conf.setString("execution.savepoint.path", "file:///Users/start/Documents/dev/flink-31/chk/684d865d69e098b41a26f58bb0aa5629/chk-54");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        //开启checkpoint
        env.enableCheckpointing(60000);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/start/Documents/dev/flink-31/chk");

        String brokerList = "node-1.51doit.cn:9092,node-2.51doit.cn:9092,node-3.51doit.cn:9092";

        KafkaSource<String> source = KafkaSource.<String>builder() //泛型指定是读取出的数据的类型
                //指定Kafka的Broker的地址
                .setBootstrapServers(brokerList)
                //指定读取的topic，可以是一个或多个
                .setTopics("tp-users")
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

        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> tpStream = lines.map(new MapFunction<String, Tuple3<Long, String, Integer>>() {
            @Override
            public Tuple3<Long, String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                long id = Long.parseLong(fields[0]);
                String name = fields[1];
                int age = Integer.parseInt(fields[2]);
                return Tuple3.of(id, name, age);
            }
        });

        String sql = "insert into tb_users (id, name, age) values (?, ?, ?)";
        //使用JDBC Sink（第一种，实现AtLeastOnce）
        SinkFunction<Tuple3<Long, String, Integer>> mysqlSink = JdbcSink.sink(
                sql, //要执行的SQL
                //将参数和preparedStatement进行映射
                new JdbcStatementBuilder<Tuple3<Long, String, Integer>>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, Tuple3<Long, String, Integer> tp) throws SQLException {
                        //将参数和preparedStatement对应的位置进行绑定
                        preparedStatement.setLong(1, tp.f0);
                        preparedStatement.setString(2, tp.f1);
                        preparedStatement.setInt(3, tp.f2);
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

        tpStream.addSink(mysqlSink);

        env.execute();

    }

}
