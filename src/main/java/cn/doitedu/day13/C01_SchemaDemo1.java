package cn.doitedu.day13;

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
import static org.apache.flink.table.api.Expressions.toTimestampLtz;

/**
 * 创建视图时，指定Schema信息（功能更强大，并且新的API）
 */
public class C01_SchemaDemo1 {

    public static void main(String[] args) throws Exception {

        //StreamExecutionEnvironment只能创建DataStream，并且调用DataStream的API
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //现在向使用SQL，使用StreamTableEnvironment将原来的StreamExecutionEnvironment增强
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //spark,1
        //hive,1
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        //对数据流进行整理
        SingleOutputStreamOperator<Tuple2<String, Integer>> tpStream = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
            }
        });


        /**
         * +------+--------+-------+-----+--------+-----------+
         * | name |   type |  null | key | extras | watermark |
         * +------+--------+-------+-----+--------+-----------+
         * |   f0 | STRING |  true |     |        |           |
         * |   f1 |    INT | false |     |        |           |
         *
         */
        //没有指定任何信息
        Schema schema = Schema.newBuilder()
                //给字段指定类型
                //.column("f0", DataTypes.STRING())
                //.column("f1", DataTypes.INT())
                //.columnByExpression("word", "f0") //重新命名
                //.columnByExpression("counts", $("f1")) //重新命名
                //.columnByExpression("word", $("f0").upperCase()) //重新命名
                .columnByExpression("word", "UPPER(f0)") //重新命名
                //.columnByExpression("counts", $("f1").cast(DataTypes.BIGINT())) //重新命名
                .columnByExpression("counts", "cast(f1 as bigint)") //重新命名
                //从元数据信息中获取字段，即将processingTime转成TIMESTAMP_LTZ，并命名为ptime
                .columnByMetadata("ptime", DataTypes.TIMESTAMP_LTZ(3), "proctime")
                .columnByMetadata("etime", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
                .build();

        //使用tableEnv将DataStream关联schema，注册成视图
        tEnv.createTemporaryView("v_wc", tpStream, schema);

        //写SQL
        TableResult tableResult = tEnv.executeSql("desc v_wc");

        //写SQL
        //TableResult tableResult = tEnv.executeSql("select word, sum(counts) total_counts from v_wc group by word");

        tableResult.print();

        env.execute();
    }
}
