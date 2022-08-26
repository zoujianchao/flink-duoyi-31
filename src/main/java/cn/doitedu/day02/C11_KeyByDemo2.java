package cn.doitedu.day02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * KeyBy的用法
 *
 * keyBy是按照指的字段的hash进行分区，类似spark的hashPartitioner
 *
 * 可以保证key相同的数据，一定进入到同一个分区中
 *
 * 2.传入封装数据Bean的字段名称
 *
 */
public class C11_KeyByDemo2 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //spark,1
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);


        SingleOutputStreamOperator<WCBean> wordAndCount = lines.map(new MapFunction<String, WCBean>() {
            @Override
            public WCBean map(String line) throws Exception {
                String[] fields = line.split(",");
                return new WCBean(fields[0], Integer.parseInt(fields[1]));
            }
        });

        //key的用法：
        //2.传入封装数据Bean的字段名称
        KeyedStream<WCBean, Tuple> keyed = wordAndCount.keyBy(0); //字段名称必须与bean中名称一致，不然没法通过反射获取到对应的值

        SingleOutputStreamOperator<WCBean> summed = keyed.sum("count");

        summed.print();

        env.execute();


    }

    public static class WCBean {

        private String word;

        private Integer count;

        public WCBean() {}

        public WCBean(String word, Integer count) {
            this.word = word;
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public Integer getCount() {
            return count;
        }

        public void setCount(Integer count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return "WCBean{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }

}
