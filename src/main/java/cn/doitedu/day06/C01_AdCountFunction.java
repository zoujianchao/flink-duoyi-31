package cn.doitedu.day06;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * Flink自定义的UDF
 */
public class C01_AdCountFunction extends KeyedProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, Tuple4<String, String, Long, Long>> {

    private transient ValueState<Long> countState;

    private transient ValueState<HashSet<String>> uidState;

    @Override
    public void open(Configuration parameters) throws Exception {
        //1.定义状态描述器
        //统计次数的状态
        ValueStateDescriptor<Long> countStateDescriptor = new ValueStateDescriptor<Long>("ad-count-state", Long.class);
        countState = getRuntimeContext().getState(countStateDescriptor);

        //保存用户ID的状态
        ValueStateDescriptor<HashSet<String>> uidStateDescriptor = new ValueStateDescriptor<>("uid-state", TypeInformation.of(new TypeHint<HashSet<String>>() {
        }));
        uidState = getRuntimeContext().getState(uidStateDescriptor);
    }

    /**
     * @param input (广告id，事件类型，用户id)
     * @param ctx
     * @param out   （广告id，事件类型, 次数，人数）
     * @throws Exception
     */
    @Override
    public void processElement(Tuple3<String, String, String> input, Context ctx, Collector<Tuple4<String, String, Long, Long>> out) throws Exception {

        //统计次数
        Long count = countState.value();
        if (count == null) {
            count = 0L;
        }
        //次数，即来一次加一次
        count += 1;
        //更新状态
        countState.update(count);

        //统计人数
        String uid = input.f2;
        HashSet<String> uidSet = uidState.value();
        if (uidSet == null) {
            uidSet = new HashSet<>();
        }
        //将当前用户添加到set中
        uidSet.add(uid);
        //更新状态
        uidState.update(uidSet);

        //输出数据
        out.collect(Tuple4.of(ctx.getCurrentKey().f0, ctx.getCurrentKey().f1, count, (long) uidSet.size()));
    }
}
