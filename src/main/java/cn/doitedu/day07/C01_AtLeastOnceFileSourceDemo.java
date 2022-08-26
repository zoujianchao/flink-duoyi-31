package cn.doitedu.day07;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.UUID;

/**
 * 自定义一个FileSource，并且使用OperatorState记录偏移量可以实现AtLeastOnce
 */
public class C01_AtLeastOnceFileSourceDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        env.enableCheckpointing(10000);

        //从socket端口号读取数据
        DataStreamSource<String> errors = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<String> mapped = errors.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                if (value.contains("error")) {
                    throw new RuntimeException("有问题数据，抛出异常");
                }
                return value;
            }
        });


        //从文件中读取数据
        DataStreamSource<String> lines = env.addSource(new AtLeastOnceFileSource("/Users/start/Desktop/data"));

        DataStream<String> union = lines.union(mapped);

        union.addSink(new RichSinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                System.out.println(value + " -> " + indexOfThisSubtask);
            }
        });

        env.execute();


    }


    /**
     * 使用OperatorState要实现一个接口CheckpointedFunction
     *
     * 先执行initializeState -> open -> run (一直运行)
     *
     * snapshotState在checkpoint时，每一次checkpoint每个subtask都会调用一次snapshotState
     *
     *
     */
    public static class AtLeastOnceFileSource extends RichParallelSourceFunction<String> implements CheckpointedFunction {

        private boolean flag = true;

        //transient瞬时的，即用该字段修饰的变量，在序列化时，该变量不参与序列化和反序列化
        private ListState<Long> offsetsState;

        private String path;

        private long offset = 0;

        //在构造方法中给path赋值
        public AtLeastOnceFileSource(String path) {
            this.path = path;

        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            //System.out.println("initializeState method invoked ###");
            //getRuntimeContext().getListState() 获取KeyedState中ListState
            //定义状态描述器
            ListStateDescriptor<Long> stateDescriptor = new ListStateDescriptor<>("offset-state", Long.class);
            offsetsState = context.getOperatorStateStore().getListState(stateDescriptor);//获取OperatorState中的ListState
            //将offsetsState中的偏移量恢复
            if(context.isRestored()) { //判断OperatorState是否已经恢复完成
                for (Long offset : offsetsState.get()) {
                    this.offset = offset;
                }
            }
        }


        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            //subtask 0 读取 0.txt
            RandomAccessFile randomAccessFile = new RandomAccessFile(path + "/" + indexOfThisSubtask + ".txt", "r");
            randomAccessFile.seek(offset); //从指定的位置读取数据

            while (flag) {
                String line = randomAccessFile.readLine();
                if (line != null) {
                    line = new String(line.getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8);
                    //输出数据
                   //synchronized (ctx.getCheckpointLock()) {
                       ctx.collect(indexOfThisSubtask + " : " + line);
                       //获取最新的偏移量
                       offset = randomAccessFile.getFilePointer();
                   //}
                } else {
                    //没有新的数据写入
                    Thread.sleep(500);
                }
            }
        }

        @Override
        public void cancel() {
            flag = false;
            //System.out.println("cancel method invoked @@@");
        }


        //在每一次checkpoint时，每个subtask会调用一次
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            //System.out.println("snapshotState method invoked $$$");
            offsetsState.clear(); //清掉上一次的偏移量
            offsetsState.add(offset); //将最新的偏移量添加的哦OperatorState中
        }


    }

}
