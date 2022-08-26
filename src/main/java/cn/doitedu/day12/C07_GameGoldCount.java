package cn.doitedu.day12;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 使用FlinkSQL统计游戏相关数据的指标
 * 解决数据倾斜的问题
 */
public class C07_GameGoldCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<GameBean> beanStream = lines.map(new MapFunction<String, GameBean>() {
            @Override
            public GameBean map(String value) throws Exception {
                String[] fields = value.split(",");
                return new GameBean(fields[0], fields[1], Integer.parseInt(fields[2]));
            }
        });

        //注册视图
        tEnv.createTemporaryView("tb_game_log", beanStream);

        TableResult tableResult = tEnv.executeSql("select\n" +
                "  gid,\n" +
                "  sum(gold) gold\n" +
                "from\n" +
                "(\n" +
                "  select\n" +
                "    gid,\n" +
                "    ri,\n" +
                "    sum(gold) gold\n" +
                "  from\n" +
                "  (\n" +
                "    select\n" +
                "      gid,\n" +
                "      uid,\n" +
                "      gold,\n" +
                "      RAND_INTEGER(10) ri\n" +
                "    from\n" +
                "      tb_game_log\n" +
                "  )\n" +
                "  group by gid, ri\n" +
                ")\n" +
                "group by gid");

        tableResult.print();

        env.execute();

    }


    public static class GameBean {

        public String gid;
        public String uid;
        public Integer gold;

        public GameBean() {}

        public GameBean(String gid, String uid, Integer gold) {
            this.gid = gid;
            this.uid = uid;
            this.gold = gold;
        }

        @Override
        public String toString() {
            return "GameBean{" +
                    "gid='" + gid + '\'' +
                    ", uid='" + uid + '\'' +
                    ", gold=" + gold +
                    '}';
        }
    }
}
