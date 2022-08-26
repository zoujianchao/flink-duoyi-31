package cn.doitedu.day12;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 使用FlinkSQL划分滑动窗口，将同一个窗口内，用户ID相同的金额进行聚合
 */
public class C09_SqlSlidingEventTimeWindows {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //1000,u1,p1,5
        //2000,u1,p1,5
        //2000,u2,p1,3
        //3000,u1,p1,5
        //4000,u1,p1,5
        //9999,u2,p1,3
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //对数据进行整理
        DataStream<Row> rowDataStream = lines.map(new MapFunction<String, Row>() {
            @Override
            public Row map(String line) throws Exception {
                String[] fields = line.split(",");
                Long time = Long.parseLong(fields[0]);
                String uid = fields[1];
                String pid = fields[2];
                Double money = Double.parseDouble(fields[3]);
                return Row.of(time, uid, pid, money);

            }
        }).returns(Types.ROW(Types.LONG, Types.STRING, Types.STRING, Types.DOUBLE));

        //提取数据中的EventTime并生成WaterMark
        DataStream<Row> waterMarksRow = rowDataStream.assignTimestampsAndWatermarks(WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofMillis(0)).withTimestampAssigner((r, t) -> (long) r.getField(0)));
        //将DataStream注册成表并指定schema信息
        //tableEnv.registerDataStream("t_orders", waterMarksRow, "time, uid, pid, money, rowtime.rowtime");

        //$("rowtime").rowtime(), 将EventTime取出来，命名为rowtime
        //$("rowtime").proctime(), 将ProcessingTime取出来，命名为rowtime
        tableEnv.createTemporaryView("t_orders", waterMarksRow, $("time"), $("uid"), $("pid"), $("money"), $("rowtime").rowtime());
        //使用SQL实现按照EventTime划分滑动窗口聚合
        String sql = "SELECT uid, SUM(money) total_money, HOP_START(rowtime, INTERVAL '2' SECOND, INTERVAL '10' SECOND) as winStart, HOP_END(rowtime, INTERVAL '2' SECOND, INTERVAL '10' SECOND) as widEnd" +
                " FROM t_orders GROUP BY HOP(rowtime, INTERVAL '2' SECOND, INTERVAL '10' SECOND), uid";
        Table table = tableEnv.sqlQuery(sql);
        //使用TableEnv将table转成AppendStream
        DataStream<Row> result = tableEnv.toAppendStream(table, Row.class);
        result.print();
        env.execute();
    }

}
