package com.imooc.scala.state

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * 使用MapWithState实现带有状态的单词计数案例
 * 思考问题：有状态的单词计数代码  和  无状态的单词计数代码有什么区别？
 * Created by xuwei
 */
object KeyedState_MapWithStateDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC)

    val text = env.socketTextStream("bigdata04", 9001)

    import org.apache.flink.api.scala._
    val keyedStream = text.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(_._1)

    /**
     * T：当前数据流中的数据类型
     * R：返回数据类型
     * S：State中存储的数据类型
     * 注意：在这里State默认是ValueState，ValueState中可以存储多种数据类型
     */
    keyedStream.mapWithState[(String,Int),Int]((in: (String,Int),count: Option[Int])=>{
      //(t1,t2)
      //t1：表示这个map操作需要返回的数据
      //t2：表示State中目前的数据
      count match {
        case Some(c) => ((in._1,in._2+c),Some(in._2+c))//第2及以上次数，返回累加后的数据，更新状态
        case None => ((in._1,in._2),Some(in._2))//第一次接收到数据，直接返回数据，初始化状态
      }
    }).print()

    env.execute("KeyedState_MapWithStateDemo")
  }

}
