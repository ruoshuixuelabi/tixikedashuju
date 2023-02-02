package com.imooc.scala.windowagg

import java.time.ZoneId

import org.apache.flink.configuration.CoreOptions
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/**
 * 滚动窗口：使用GroupWindowAggregation实现
 *
 * 支持流处理和批处理任务
 * Created by xuwei
 */
object TumbleWindowByGroupWindowAggSQL {
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
    val execSql =
      """
        |SELECT
        |  order_type,
        |  COUNT(*) AS order_cnt,
        |  SUM(price) AS price_sum,
        |  -- 窗口开始时间
        |  TUMBLE_START(order_time,INTERVAL '10' SECOND) AS window_start,
        |  -- 窗口结束时间
        |  TUMBLE_END(order_time,INTERVAL '10' SECOND) AS window_end
        |FROM orders_source
        |GROUP BY
        |  -- TUMBLE代表是滚动窗口，第一个参数是时间字段，第二个参数是滚动窗口大小(10秒)
        |  TUMBLE(order_time,INTERVAL '10' SECOND),
        |  order_type
        |""".stripMargin
    tEnv.executeSql(execSql).print()

  }

}
