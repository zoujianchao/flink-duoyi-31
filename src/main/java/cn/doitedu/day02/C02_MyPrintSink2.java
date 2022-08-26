package cn.doitedu.day02;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * 自定义PrintSink ，实现类似print()的功能
 */
public class C02_MyPrintSink2 {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(8);

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<String> upper = lines.map(l -> l.toUpperCase()).setParallelism(2);

        //upper.print();

        upper.addSink(new MyPrintSink());

        env.execute();


    }

    public static class MyPrintSink extends RichSinkFunction<String> {


        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        //Sink没接收到一条数据，就调用一次
        @Override
        public void invoke(String value, Context context) throws Exception {
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            System.out.println((indexOfThisSubtask + 1) + " > " + value);
        }

        @Override
        public void close() throws Exception {
            super.close();
        }
    }

}
