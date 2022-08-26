package cn.doitedu.day12;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 使用SQL的方式，实现滚动窗口聚合
 *
 * //不划分窗口
 * select
 *   uid,
 *   sum(money) total_money
 * from
 *   tb_orders
 * group by uid
 *
 *
 * //划分窗口，并且将同一个窗口内的数据，用户id相同的数据进行聚合
 * select
 *   uid,
 *   sum(money) total_money
 * from
 *   tb_orders
 * group by TUMBLE(etime, INTERVAL '10' SECOND), uid
 *
 *
 */
public class C08_SqlTumblingEventTimeWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //为了触发方便，并行度设置为1
        env.setParallelism(1);

        //如果希望能够容错，要开启checkpoint
        env.enableCheckpointing(10000);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //时间,用户id,商品id,金额
        //1000,u1,p1,5
        //2000,u1,p1,5
        //2000,u2,p1,3
        //2000,u2,p2,8
        //9999,u1,p1,5
        //18888,u2,p1,3
        //19999,u2,p1,3
        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 8888);

        //对数据进行转换
        SingleOutputStreamOperator<Row> rowDataStream = socketTextStream.map(
                new MapFunction<String, Row>() {
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
        DataStream<Row> waterMarksRow = rowDataStream.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<Row>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(Row row) {
                        return (long) row.getField(0);
                    }
                });
        //将DataStream注册成表并指定schema信息
        //$("etime").rowtime()，将数据流中的EVENT Time取名为etime
        tableEnv.createTemporaryView("t_orders", waterMarksRow, $("time"), $("uid"), $("pid"), $("money"), $("etime").rowtime());

        //使用DataStream的API
        //waterMarksRow.keyBy(t -> t.getField(1)).window(TumblingEventTimeWindows.of(Time.seconds(10))).sum(3)

        //将同一个滚动窗口内，相同用户id的money进行sum操作(没有取出窗口的起始时间、结束时间)
        //String sql = "SELECT uid, SUM(money) total_money FROM t_orders GROUP BY TUMBLE(etime, INTERVAL '10' SECOND), uid";
        String sql = "select\n" +
                "  TUMBLE_START(etime, INTERVAL '10' SECOND) win_start,\n" +
                "  TUMBLE_END(etime, INTERVAL '10' SECOND) win_end,\n" +
                "  uid,\n" +
                "  sum(money) total_money\n" +
                "from\n" +
                "  t_orders\n" +
                "group by TUMBLE(etime, INTERVAL '10' SECOND), uid\n";

        TableResult tableResult = tableEnv.executeSql(sql);

        tableResult.print();

        env.execute();
    }

}
