package com.imooc.scala.sql

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/**
 * Source：FileSystem
 * Sink：Print
 *
 * Created by xuwei
 */
object FileSystemPrintSQL {
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

    //创建输出表
    val outTableSql =
      """
        |CREATE TABLE print_sink(
        |  age INT,
        |  cnt BIGINT
        |)WITH(
        |  'connector' = 'print'
        |)
        |""".stripMargin
    tEnv.executeSql(outTableSql)

    //业务逻辑
    val execSql =
      """
        |INSERT INTO print_sink
        |SELECT
        |  age,
        |  COUNT(*) AS cnt
        |FROM file_source
        |GROUP BY age
        |""".stripMargin
    tEnv.executeSql(execSql)
  }

}
