package com.imooc.scala.state

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

import scala.collection.mutable
/**
 * 配置数据流-自定义Soource
 * Created by xuwei
 */
class MyRedisSource extends RichSourceFunction[mutable.Map[String,String]]{
  var isRunning = true

  /**
   * 初始化方法，只执行一次
   * @param parameters
   */
  override def open(parameters: Configuration): Unit = {
    //TODO 创建Redis数据库连接
  }

  /**
   * Source的核心方法，负责源源不断的产生数据
   * @param ctx
   */
  override def run(ctx: SourceFunction.SourceContext[mutable.Map[String, String]]): Unit = {
    while (isRunning){
      // TODO 需要从Redis中获取这些映射关系
      val resMap = mutable.Map("US"->"AREA_US","PK"->"AREA_AR","KW"->"AREA_AR")
      ctx.collect(resMap)
      Thread.sleep(5000)//每隔5秒更新一次配置数据
    }
  }

  /**
   * 任务停止的时候执行一次
   * 这里主要负责控制run方法中的循环
   */
  override def cancel(): Unit = {
    isRunning = false
  }

  /**
   * 任务停止的时候执行一次
   * 这里主要负责关闭在open方法中创建的连接
   */
  override def close(): Unit = {
    //TODO 关闭Redis数据库连接
  }
}
