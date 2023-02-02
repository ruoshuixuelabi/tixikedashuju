package com.imooc.scala.state

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * 通过自定义Sink实现批量输出
 * Created by xuwei
 */
object OperatorState_MyBufferSinkDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC)
    //设置并行度为2
    env.setParallelism(2)

    val text = env.socketTextStream("bigdata04", 9001)

    import org.apache.flink.api.scala._
    text.flatMap(_.split(" "))
      .map((_,1))
      .addSink(new MyBufferSink())

    env.execute("OperatorState_MyBufferSinkDemo")
  }

}
