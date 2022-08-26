package cn.doitedu.day02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;

/**
 * 使用Map实时关联MySQL中的维度数据
 */
public class C04_MapDemo3 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //订单id,商品分类
        //o100,1
        //o101,2
        DataStreamSource<String> orders = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple3<String, Integer, String>> res = orders.map(new CategoryNameMapFunction());


        //处理后的数据
        //o100,1,家具
        //o101,2,手机

        res.print();

        env.execute();

    }


    public static class CategoryNameMapFunction extends RichMapFunction<String, Tuple3<String, Integer, String>> {

        private Connection connection;

        @Override
        public void open(Configuration parameters) throws Exception {
            //建立MySQL连接
            connection = DriverManager.getConnection("jdbc:mysql://node-1.51doit.cn:3306/doit31?characterEncoding=utf-8", "root", "123456");
        }

        @Override
        public Tuple3<String, Integer, String> map(String line) throws Exception {
            String[] fields = line.split(",");
            //使用分类id查询MySQL
            int cid = Integer.parseInt(fields[1]);
            String name = "未知";
            PreparedStatement preparedStatement = null;
            ResultSet resultSet = null;
            try {
                preparedStatement = connection.prepareStatement("select name from t_order_count where cid = ?");
                preparedStatement.setInt(1, cid);
                resultSet = preparedStatement.executeQuery();

                if (resultSet.next()) {
                    name = resultSet.getString(1);
                }
            } finally {
                if (resultSet != null) resultSet.close();
                if (preparedStatement != null) preparedStatement.close();
            }
            return Tuple3.of(fields[0], cid, name);
        }

        @Override
        public void close() throws Exception {
            //关闭连接
            connection.close();
        }
    }
}
