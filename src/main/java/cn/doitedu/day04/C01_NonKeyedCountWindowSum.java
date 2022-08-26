package cn.doitedu.day04;

import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * Flink 中的窗口，分为两种，CountWindow，TimeWindow
 *
 * CountWindow是按照数据的条数划分窗口
 * TimeWindow是按照时间划分窗口
 *
 * Window按照是否进行了KeyBy
 *
 *   key后划分的窗口：KeyedWindow，调用的是window
 *   没有keyBy划分窗口：NonKeyedWindow，底层调用的是windowAll方法
 *
 * 没有keyBy的Window，即NonKeyedWindow，Window和WindowOperator所在的DataStream，并行度为1，如果数据量比较多，会导致数据倾斜
 *
 */
public class C01_NonKeyedCountWindowSum {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1
        //2
        //3
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //将数据转成integer
        SingleOutputStreamOperator<Integer> nums = lines.map(Integer::parseInt);

        //没有keyBy,直接划分窗口
        //countWindowAll没有keyBy，按照条数划分的窗口
        AllWindowedStream<Integer, GlobalWindow> windowStream = nums.countWindowAll(5);

        //划分窗口后，对窗口中的数据进行操作，sum、reduce、apply（WindowOperator，即对window进行处理的算子）
        SingleOutputStreamOperator<Integer> summed = windowStream.sum(0);

        summed.print();

        env.execute();


    }

}
