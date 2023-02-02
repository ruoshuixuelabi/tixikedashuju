package com.imooc.scala.windowagg

import java.time.ZoneId

import org.apache.flink.configuration.CoreOptions
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/**
 * 滑动窗口：使用WindowingTVF实现
 *
 * 只支持流处理任务
 * Created by xuwei
 */
object HopWindowByWindowingTVFSQL {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val settings = EnvironmentSettings
      .newInstance()
      //指定执行模式，支持inBatchMode和inStreamingMode
      .inStreamingMode()
      .build()
    val tEnv = TableEnvironment.create(settings)

    //指定国内的时区
    tEnv.getConfig.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))

    //设置全局并行度为1
    tEnv.getConfig.set(CoreOptions.DEFAULT_PARALLELISM.key(),"1")

    //创建输入表
    val inTableSql =
      """
        |CREATE TABLE orders_source(
        |    order_id    BIGINT,
        |    order_type  STRING,
        |    price       DECIMAL(10,2),
        |    -- 定义一个时间字段，使用数据处理时间
        |    order_time  AS PROCTIME()
        |) WITH (
        |    'connector' = 'datagen',
        |    'rows-per-second' = '1',
        |    'fields.order_type.length' = '1'
        |)
        |""".stripMargin
    tEnv.executeSql(inTableSql)

    //业务逻辑
    val execSql2 =
      """
        |SELECT
        |  order_type,
        |  COUNT(*) AS order_cnt,
        |  SUM(price) AS price_sum,
        |  -- 窗口开始时间
        |  window_start,
        |  -- 窗口结束时间
        |  window_end
        |FROM TABLE(-- 把HOP WINDOW的信息定义在数据源的TABLE子句中
        |  HOP(
        |    TABLE orders_source, -- 指定输入表名称
        |    DESCRIPTOR(order_time), -- 指定时间字段
        |    INTERVAL '5' SECOND,  -- 指定滑动窗口步长
        |    INTERVAL '10' SECOND -- 指定滑动窗口的大小
        |  )
        |)
        |GROUP BY
        |  window_start,
        |  window_end,
        |  order_type
        |""".stripMargin

    tEnv.executeSql(execSql2).print()
  }

}
