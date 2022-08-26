package cn.doitedu.day12;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 在FlinkSQL中自定义功能函数
 *
 * 自定义一个可以输入ip地址，返回省份信息的函数（实时）
 *
 */
public class C11_UDFSQLDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //在客户端注册一个可以Cache的文件,通过网络发送给TaskManager（类似spark的广播变量，广播不变得数据）
        env.registerCachedFile("/Users/start/Documents/dev/flink-31/doc/ip.txt", "ip-rules");

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //106.121.4.252
        //42.57.88.186
        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 8888);

        tableEnv.createTemporaryView("t_lines", socketTextStream, $("ip"));

        //注册自定义函数，是一个UDF,输入一个IP地址，返回Row<省、市>
        tableEnv.createTemporarySystemFunction("ipToLocation", IpLocation.class);

        Table table = tableEnv.sqlQuery(
                "SELECT ip, ipToLocation(ip) location FROM t_lines");

        tableEnv.toDataStream(table, Row.class).print();

        env.execute();

    }

}
