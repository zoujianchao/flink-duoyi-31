package cn.doitedu.day06;

/**
 * Flink总共有两种状态KeyedState和OperatorState
 *
 * 只有KeyedState有TTL（Time To Live）
 *   ValueState、MapState、ListState可以设置TTL
 *
 * 先使用ValueState<Integer>
 *
 *   CowMap<Key, Integer> 为 ValueState设置TTL，是将对Key和V设置TTL
 *
 */
public class C05_ValueStateTTLDemo1 {

    public static void main(String[] args) {




    }

}
