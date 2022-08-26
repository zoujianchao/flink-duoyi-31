package cn.doitedu.day12;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * FlinkSQL的自定义函数
 *
 * UDF
 *
 */
public class C10_UDFDemo1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //province,city
        //辽宁省,沈阳市
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple2<String, String>> tpStream = lines.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[0], fields[1]);
            }
        });

        tEnv.createTemporaryView("v_data", tpStream);

        //先注册函数
        tEnv.createTemporaryFunction("My_CONCAT_WS", MyConcatWs.class);

        TableResult tableResult = tEnv.executeSql("SELECT My_CONCAT_WS('-', f0, f1) location from v_data");

        tableResult.print();

        env.execute();

    }

    public static class MyConcatWs extends ScalarFunction {

        //方法名必须为evel
        public String eval(String sp, String f1, String f2) {
            return f1 + sp + f2;
        }

    }

}
