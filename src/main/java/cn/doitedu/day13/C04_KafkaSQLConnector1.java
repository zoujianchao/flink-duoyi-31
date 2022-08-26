package cn.doitedu.day13;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 使用SQL方法，创建Kafka的Source，从Kafka中读取数据
 */
public class C04_KafkaSQLConnector1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //u01,i101,view
        //执行SQL创建一个Source表
        tEnv.executeSql("CREATE TABLE tb_events (\n" +
                "  `user_id` BIGINT,\n" +
                "  `item_id` BIGINT,\n" +
                "  `behavior` STRING,\n" +
                "  `ts` TIMESTAMP(3)  METADATA FROM 'timestamp' \n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'tp-events',\n" +
                "  'properties.bootstrap.servers' = 'node-1.51doit.cn:9092,node-2.51doit.cn:9092,node-3.51doit.cn:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'csv', \n" +
                "  'csv.ignore-parse-errors' = 'true' \n" +  //忽略解析出错的数据，对应的字段为NULL
                ")");

        //KafkaSource -> Map（整理数据）-> 过滤
        TableResult tableResult = tEnv.executeSql("SELECT user_id, item_id, behavior, ts FROM tb_events WHERE user_id IS NOT NULL AND behavior <> 'pay'");

        tableResult.print();

        env.execute();
    }
}
