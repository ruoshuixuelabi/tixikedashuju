package com.imooc.scala

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 单词计数案例
 * 注意：基于最新Flink批流一体化的API开发
 * Created by xuwei
 */
object WordCountScala {
  def main(args: Array[String]): Unit = {
    //获取执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //指定处理模式，默认支持流处理模式，也支持批处理模式
    /**
     * STREAMING：流处理模式，默认。
     * BATCH：批处理模式。
     * AUTOMATIC：让系统根据数据源是否有界来自动判断是使用STREAMING还是BATCH
     *
     * 建议在客户端中使用flink run提交任务的时候通过-Dexecution.runtime-mode=BATCH指定
     */
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC)

    //指定DataSource
    val text = env.socketTextStream("bigdata04", 9001) //当处理模式指定为AUTOMATIC，会按照流处理模式执行
    //val text = env.readTextFile("D:\\data\\hello.txt")//当处理模式指定为AUTOMATIC，会按照批处理模式执行

    //指定具体的业务逻辑
    import org.apache.flink.api.scala._
    val wordCount = text.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(_._1)
      //针对时间窗口，目前官方建议使用window，主要是为了让用户指定窗口触发是使用处理时间 or 事件时间
      .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
      .sum(1)

    //指定DataSink
    wordCount.print().setParallelism(1)

    //执行程序
    env.execute("WordCountScala")
  }

}
