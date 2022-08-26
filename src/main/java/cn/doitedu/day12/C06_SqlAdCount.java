package cn.doitedu.day12;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 使用FlinkSQL统计广告的各种事件对应的人数和次数
 */
public class C06_SqlAdCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //为了可以容错，必须开启checkpoint
        env.enableCheckpointing(10000);
        //指定StateBackend
        env.setStateBackend(new HashMapStateBackend());
        //指定StateBackend的数据存储的位置
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/start/Documents/dev/flink-31/chk");

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //a001,view,u001
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<AdBean> beanStream = lines.map(new MapFunction<String, AdBean>() {
            @Override
            public AdBean map(String value) throws Exception {
                if (value.contains("error")) {
                    throw new RuntimeException("数据有问题!!!");
                }
                String[] fields = value.split(",");
                return new AdBean(fields[0], fields[1], fields[2]);
            }
        });

        //注册视图
        tEnv.createTemporaryView("tb_events", beanStream);

        TableResult tableResult = tEnv.executeSql("select aid, event, count(*) total_counts, count(distinct uid) u_counts from tb_events group by aid, event");

        tableResult.print();

        env.execute();

    }


    public static class AdBean {

        public String aid;
        public String event;
        public String uid;

        public AdBean() {}

        public AdBean(String aid, String event, String uid) {
            this.aid = aid;
            this.event = event;
            this.uid = uid;
        }

        @Override
        public String toString() {
            return "AdBean{" +
                    "aid='" + aid + '\'' +
                    ", event='" + event + '\'' +
                    ", uid='" + uid + '\'' +
                    '}';
        }
    }
}
