package cn.doitedu.day08;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Flink的侧流输出（旁路输出）
 * 本质上，就是将一个DataStream中的数据打上一个或多个标签，然后根据需要，将想要的数据，取出对应标签的数据
 *
 * 将数据根据奇数、偶数、字符串，打上三种标签，然后根据不同的标签，取出不同的数据
 *
 * 侧流输出的优势，比filter的效率高（filter多次过滤，需要将同样的数据进行拷贝）
 */
public class C09_SideOutputDemo1 {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1
        //2
        //abc
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //定义标签
        //奇数
        OutputTag<Integer> oddTag = new OutputTag<Integer>("odd-tag"){};
        //偶数
        OutputTag<Integer> evenTag = new OutputTag<Integer>("even-tag"){};
        //字符串
        OutputTag<String> strTag = new OutputTag<String>("str-tag"){};

        //数据按照是否打标签，分为主流和非主流
        SingleOutputStreamOperator<String> mainStream = lines.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {

                try {
                    int num = Integer.parseInt(value);
                    //数字类型
                    if (num % 2 != 0) {
                        //奇数
                        ctx.output(oddTag, num); //侧流输出
                    } else {
                        //偶数
                        ctx.output(evenTag, num); //侧流输出
                    }
                } catch (NumberFormatException e) {
                    //字符串类型
                    ctx.output(strTag, value); //侧流输出
                }

                //输出为打标签的数据（主流中）
                out.collect(value);
            }
        });

        //获取打标签的数据
        DataStream<Integer> oddStream = mainStream.getSideOutput(oddTag);

        DataStream<String> strStream = mainStream.getSideOutput(strTag);

        oddStream.print("odd: ");
        strStream.print("str: ");

        //输出没有打标签的数据
        mainStream.print("main: ");

        env.execute();

    }
}
