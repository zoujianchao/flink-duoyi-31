package cn.doitedu.day12;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 使用RetractStream（老的）和ChangelogStream（新的）
 *
 * 适用于聚合操作
 */
public class C05_StreamSQLWordCount2 {

    public static void main(String[] args) throws Exception {

        //StreamExecutionEnvironment只能创建DataStream，并且调用DataStream的API
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //现在向使用SQL，使用StreamTableEnvironment将原来的StreamExecutionEnvironment增强
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //spark,1
        //hive,1
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        //对数据流进行整理
        //对数据流进行整理
        SingleOutputStreamOperator<C04_StreamSQLWordCount.DataBean> tpStream = lines.map(new MapFunction<String, C04_StreamSQLWordCount.DataBean>() {
            @Override
            public C04_StreamSQLWordCount.DataBean map(String value) throws Exception {
                String[] fields = value.split(",");
                return new C04_StreamSQLWordCount.DataBean(fields[0], Integer.parseInt(fields[1]));
            }
        });


        //使用tableEnv将DataStream关联schema，注册成视图
        tEnv.createTemporaryView("v_wc", tpStream);

        //执行查询
        Table table = tEnv.sqlQuery("select word, sum(counts) total_counts from v_wc group by word");

        //tEnv.toDataStream(table) 不支持回退的，只支持插入（append）

        //老的API
        DataStream<Tuple2<Boolean, Row>> res = tEnv.toRetractStream(table, Row.class);



        //新的API
        //DataStream<Row> res = tEnv.toChangelogStream(table);

        res.print();

        env.execute();
    }


}
