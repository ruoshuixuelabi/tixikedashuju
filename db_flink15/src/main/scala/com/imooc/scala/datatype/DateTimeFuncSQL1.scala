package com.imooc.scala.datatype

import java.time.ZoneId

import org.apache.flink.configuration.CoreOptions
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/**
 * 日期相关函数的使用
 * Created by xuwei
 */
object DateTimeFuncSQL1 {
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

    val querySQL =
      """
        |SELECT
        |CURRENT_DATE,
        |LOCALTIME,
        |CURRENT_TIME,
        |LOCALTIMESTAMP,
        |CURRENT_TIMESTAMP,
        |CURRENT_ROW_TIMESTAMP(),
        |NOW(),
        |PROCTIME(),
        |DATE_FORMAT(CURRENT_TIMESTAMP,'yyyy-MM-dd HH:mm:ss')
        |""".stripMargin

    tEnv.executeSql(querySQL).print()
  }

}
