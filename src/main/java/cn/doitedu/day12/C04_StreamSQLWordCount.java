package cn.doitedu.day12;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 使用FlinkSQL实现流式的WordCount
 * 使用老的API注册视图
 */
public class C04_StreamSQLWordCount {

    public static void main(String[] args) throws Exception {

        //StreamExecutionEnvironment只能创建DataStream，并且调用DataStream的API
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //现在向使用SQL，使用StreamTableEnvironment将原来的StreamExecutionEnvironment增强
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //spark,1
        //hive,1
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        //对数据流进行整理
        SingleOutputStreamOperator<DataBean> tpStream = lines.map(new MapFunction<String, DataBean>() {
            @Override
            public DataBean map(String value) throws Exception {
                String[] fields = value.split(",");
                return new DataBean(fields[0], Integer.parseInt(fields[1]));
            }
        });


        //使用tableEnv将DataStream关联schema，注册成视图
        tEnv.createTemporaryView("v_wc", tpStream);

        /**
         * +--------+--------+------+-----+--------+-----------+
         * |   name |   type | null | key | extras | watermark |
         * +--------+--------+------+-----+--------+-----------+
         * |   word | STRING | true |     |        |           |
         * | counts |    INT | true |     |        |           |
         * +--------+--------+------+-----+--------+-----------+
         */
        //TableResult tableResult = tEnv.executeSql("desc v_wc");

        //写SQL
        TableResult tableResult = tEnv.executeSql("select word, sum(counts) total_counts from v_wc group by word");

        tableResult.print();

        env.execute();
    }

    public static class DataBean {

        public String word;
        public Integer counts;

        public DataBean(){}

        public DataBean(String word, Integer counts) {
            this.word = word;
            this.counts = counts;
        }

        @Override
        public String toString() {
            return "DataBean{" +
                    "word='" + word + '\'' +
                    ", counts=" + counts +
                    '}';
        }
    }
}
