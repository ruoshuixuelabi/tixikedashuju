package com.imooc.scala.state

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

import scala.util.Random

/**
 * 事件数据流-自定义Source
 * Created by xuwei
 */
class MyStreamSource extends RichSourceFunction[String]{
  var isRunning = true

  /**
   * 初始化方法，只执行一次
   * @param parameters
   */
  override def open(parameters: Configuration): Unit = {

  }

  /**
   * Source的核心方法，负责源源不断的产生数据
   * @param ctx
   */
  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    while (isRunning){
      //{"dt":"2026-01-01 10:11:11","countryCode":"US","data":[{"type":"s1","score":0.3,"level":"A"},{"type":"s2","score":0.2,"level":"B"}]}
      val time = sdf.format(new Date)
      val line_prefix = "{\"dt\":\""+time+"\",\"countryCode\":\""
      val line_suffix = "\",\"data\":[{\"type\":\"s1\",\"score\":0.3,\"level\":\"A\"},{\"type\":\"s2\",\"score\":0.2,\"level\":\"B\"}]}"
      val countryCodeArr = Array("US","PK","KW")
      val num = Random.nextInt(3)
      ctx.collect(line_prefix+countryCodeArr(num)+line_suffix)
      Thread.sleep(1000)//每隔1秒产生一条数据，控制一下数据产生速度
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

  }
}
