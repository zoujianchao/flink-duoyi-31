package cn.doitedu.day01

import org.apache.flink.streaming.api.scala._

object C01_StreamingWordCount {

  def main(args: Array[String]): Unit = {

    //1.创建SparkContext（离线）、StreamingContext（流计算）

    //2.创建抽象数据集（RDD、DStream）

    //3.调用Transformation(s),每调用一次Transformation，会生成一个新的抽象数据集

    //4.调用Action

    //5.如果是SparkStreaming，要启动，然后将程序挂起

    //------------------------------------------
    //Flink程序

    //1.创建执行环境（上下文）
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //2.调用Source方法创建DataStream
    val lines: DataStream[String] = env.socketTextStream("localhost", 8888)

    //import org.apache.flink.streaming.api.scala._
    //3.调用Transformation(s)
    val words: DataStream[String] = lines.flatMap(_.split(" "))

    val wordAndOne: DataStream[(String, Int)] = words.map((_, 1))
    //按照指定的条件进行实时的分区,key相同的一定会进入到同一个分区，但是同一个分区中可能会有多个key
    val keyed: KeyedStream[(String, Int), String] = wordAndOne.keyBy(_._1)

    //聚和
    val summed: DataStream[(String, Int)] = keyed.sum(1)

    //4.调用Sink
    summed.print()

    //5.启动执行（一直运行）
    env.execute()


  }

}
