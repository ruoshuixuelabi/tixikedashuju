package com.imooc.scala.datatype

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/**
 * 日期相关函数的使用
 * 对比：CURRENT_TIMESTAMP 和 CURRENT_ROW_TIMESTAMP()之间的区别
 * Created by xuwei
 */
object DateTimeFuncSQL2 {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val settings = EnvironmentSettings
      .newInstance()
      //指定执行模式，支持inBatchMode和inStreamingMode
      .inStreamingMode()
      .build()
    val tEnv = TableEnvironment.create(settings)

    //创建输入表
    val inTableSql =
      """
        |CREATE TABLE file_source(
        |  name STRING,
        |  age INT
        |)WITH(
        |  'connector' = 'filesystem',
        |  'path' = 'hdfs://bigdata01:9000/stu.json',
        |  'format' = 'json',
        |  'json.fail-on-missing-field' = 'false',
        |  'json.ignore-parse-errors' = 'true'
        |)
        |""".stripMargin
    tEnv.executeSql(inTableSql)

    val querySQL =
      """
        |SELECT
        |  name,
        |  age,
        |  CURRENT_TIMESTAMP,
        |  CURRENT_ROW_TIMESTAMP()
        |FROM file_source
        |""".stripMargin

    tEnv.executeSql(querySQL).print()

  }

}
