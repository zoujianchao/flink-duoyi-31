package cn.doitedu.day13;

import org.apache.commons.io.FileUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.File;

/**
 * 使用SQL方法，创建Kafka的Source，从Kafka中读取数据
 */
public class C05_ReadSQLFileDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //读取SQL文件中的内容
        String line = FileUtils.readFileToString(new File(args[0]), "UTF-8");
        String[] sqls = SQLHolder.split(line);
        for (String sql : sqls) {
            tEnv.executeSql(sql);
        }

    }
}
