package cn.doitedu.day01;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;
import java.util.UUID;

/**
 * 功能更新丰富的并行的Source
 */
public class C15_RichParallelSourceDemo {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8082); //指定本地webUI服务的端口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        DataStreamSource<String> lines = env.addSource(new MyParallelSource());

        System.out.println("继承了RichParallelSourceFunction接口的Source的并行度：" + lines.getParallelism());

        lines.print();

        env.execute();


    }

    //RichParallelSourceFunction中方法的执行顺序（方法的生命周期）
    //没有小task都会按照指定顺序执行：先执行一次open -> run,当将job cancle时，会先调用cancle -> close
    //可以在open方法中做一些准备工作，创建连接
    //可以在close方法中做一些收尾工作，释放连接或资源
    public static class MyParallelSource extends RichParallelSourceFunction<String> {

        private boolean flag = true;

        @Override
        public void open(Configuration parameters) throws Exception {
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            System.out.println("!!!!!!! open method invoked in subtask : " + indexOfThisSubtask);
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            System.out.println("@@@@@@@@ run method invoked in subtask : " + indexOfThisSubtask);
            while (flag) {
                ctx.collect(UUID.randomUUID().toString());
                Thread.sleep(2000);
            }
        }

        @Override
        public void cancel() {
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            System.out.println("##### cancel method invoked in subtask : " + indexOfThisSubtask);
            flag = false;
        }

        @Override
        public void close() throws Exception {
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            System.out.println("$$$$$$ close method invoked in subtask : " + indexOfThisSubtask);
        }
    }
}
