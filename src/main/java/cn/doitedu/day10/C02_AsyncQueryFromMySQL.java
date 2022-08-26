package cn.doitedu.day10;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class C02_AsyncQueryFromMySQL {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1
        //2
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        int capacity = 20;
        DataStream<Tuple2<String, String>> result = AsyncDataStream.orderedWait(
                lines, //输入的数据流
                new C02_MySQLAsyncFunction(capacity), //异步查询的Function实例
                3000, //超时时间
                TimeUnit.MILLISECONDS, //时间单位
                capacity); //最大异步并发请求数量
        result.print();
        env.execute();

    }
}
