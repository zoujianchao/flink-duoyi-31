package cn.doitedu.day12;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 使用FlinkSQL实现流式的WordCount
 * 使用新的API注册视图（使用Schema）
 */
public class C05_StreamSQLWordCount {

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

        //指定字段的名称和类型的
        Schema schema = Schema.newBuilder()
                //.column("word", DataTypes.STRING())
                //.column("counts", DataTypes.INT())
                .columnByExpression("word1", $("word"))
                .columnByExpression("counts2", $("counts"))
                .build();

        //使用tableEnv将DataStream关联schema，注册成视图
        tEnv.createTemporaryView("v_wc", tpStream, schema);

        TableResult tableResult = tEnv.executeSql("desc v_wc");

        //写SQL
        //TableResult tableResult = tEnv.executeSql("select word, sum(counts) total_counts from v_wc group by word");

        tableResult.print();

        env.execute();
    }


}
