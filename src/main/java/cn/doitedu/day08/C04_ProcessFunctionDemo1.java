package cn.doitedu.day08;

import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * ProcessFunction是Flink更加底层的方法，可以访问Flink程序底层的属性和方法
 * 优点：更灵活
 * 缺点：使用起来更复杂
 *
 * ProcessFunction有三种功能
 * 1.对数据进行一条一条的处理
 * 2.对KeyedStream使用KeyedState
 * 3.对KeyedStream使用定时器（可以实现类似窗口的功能）
 *
 * 演示将数据来一条处理一条
 */
public class C04_ProcessFunctionDemo1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //{"name": "tom", "age": 18, "fv":99.99}
        //{"name": "tom", "age": 18, "fv":99.99
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //将json字符串进行解析，然后过滤掉问题数据
        SingleOutputStreamOperator<Boy> process = lines.process(new ProcessFunction<String, Boy>() {

            @Override
            public void processElement(String value, Context ctx, Collector<Boy> out) throws Exception {

                try {
                    Boy boy = JSON.parseObject(value, Boy.class);
                    //将数据输出
                    out.collect(boy);
                } catch (Exception e) {
                    //将问题数据保起来
                }

            }
        });

        process.print();

        env.execute();

    }


    public static class Boy {

        public String name;
        public Integer age;
        public Double fv;

        public Boy() {}

        public Boy(String name, Integer age, Double fv) {
            this.name = name;
            this.age = age;
            this.fv = fv;
        }

        @Override
        public String toString() {
            return "Boy{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    ", fv=" + fv +
                    '}';
        }
    }
}
