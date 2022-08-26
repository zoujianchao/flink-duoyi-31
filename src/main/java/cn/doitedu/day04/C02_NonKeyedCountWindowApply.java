package cn.doitedu.day04;

import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * 窗口的聚合类的算子，sum、min、reduce，都是在窗口内进行增量聚合，没有达到窗口的触发条件，窗口不触发
 * 但是，会将数据进行增量聚合（节省资源），当窗口触发后，会输出结果
 *
 *
 * 如果调研apply方法，该方法是将窗口内的数据攒起来（WindowState），当窗口触发后，在执行相应的运算逻辑
 *
 * 没有keyBy的Window，即NonKeyedWindow，Window和WindowOperator所在的DataStream，并行度为1，如果数据量比较多，会导致数据倾斜
 *
 */
class C02_NonKeyedCountWindowApply {

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

        //sum是在窗口内进行增量聚合
        //apply是将窗口内的数据全部攒起来（WindowState）
        SingleOutputStreamOperator<Integer> resStream = windowStream.apply(new AllWindowFunction<Integer, Integer, GlobalWindow>() {

            /**
             * apply方法在窗口触发后，才会调用
             * @param window 窗口的类型
             * @param input 输出的数据，缓存在State中了，当窗口触发后，才会调用apply方法，
             * @param out
             * @throws Exception
             */
            @Override
            public void apply(GlobalWindow window, Iterable<Integer> input, Collector<Integer> out) throws Exception {

                int sum = 0;
                for (Integer i : input) {
                    sum += i;
                }
                //输出数据
                out.collect(sum);
            }
        });

        resStream.print();

        env.execute();


    }

}
