package com.imooc.scala.datatype

import java.time.ZoneId

import org.apache.flink.configuration.CoreOptions
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/**
 * 对比常见的日期格式之间的区别
 * Created by xuwei
 */
object DateTimeSQL {
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

    val inTableSql =
      """
        |CREATE TABLE date_table(
        |    d_date  DATE,
        |    d_time  TIME,
        |    d_timestamp TIMESTAMP(3),
        |    d_timestamp_ltz TIMESTAMP_LTZ(3)
        |) WITH (
        |    'connector' = 'datagen',
        |    'rows-per-second' = '1'
        |)
        |""".stripMargin
    tEnv.executeSql(inTableSql)

    //查看输入表中的数据
    val querySQL =
      """
        |SELECT
        |   d_date,
        |   d_time,
        |   d_timestamp,
        |   d_timestamp_ltz,
        |   d_timestamp_ltz + INTERVAL '1' SECOND AS d_timestamp_ltz_second,
        |   d_timestamp_ltz + INTERVAL '1' MINUTE AS d_timestamp_ltz_minute,
        |   d_timestamp_ltz + INTERVAL '1' HOUR AS d_timestamp_ltz_hour,
        |   d_timestamp_ltz + INTERVAL '1' DAY AS d_timestamp_ltz_day,
        |   d_timestamp_ltz + INTERVAL '1 02' DAY TO HOUR AS d_timestamp_ltz_day_to_hour,
        |   d_timestamp_ltz + INTERVAL '02:03' HOUR TO MINUTE AS d_timestamp_ltz_hour_to_minute
        |FROM date_table
        |""".stripMargin
    tEnv.executeSql(querySQL).print()
  }

}
