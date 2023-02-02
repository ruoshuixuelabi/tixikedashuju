package com.imooc.scala.sql

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/**
 * Source： Kafka
 * Sink：Kafka
 *
 * Created by xuwei
 */
object KafkaSourceSinkSQL1 {
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
        |CREATE TABLE kafka_source(
        |  name STRING,
        |  age INT
        |)WITH(
        |  'connector' = 'kafka',
        |  'topic' = 'dt001',
        |  'properties.bootstrap.servers' = 'bigdata01:9092,bigdata02:9092,bigdata03:9092',
        |  'properties.group.id' = 'gid-sql-1',
        |  'scan.startup.mode' = 'group-offsets',
        |  'format' = 'json',
        |  'json.fail-on-missing-field' = 'false',
        |  'json.ignore-parse-errors' = 'true'
        |)
        |""".stripMargin
    tEnv.executeSql(inTableSql)

    //创建输出表
    val outTableSql =
      """
        |CREATE TABLE kafka_sink(
        |  age INT,
        |  cnt BIGINT
        |)WITH(
        |  'connector' = 'kafka',
        |  'topic' = 'dt002',
        |  'properties.bootstrap.servers' = 'bigdata01:9092,bigdata02:9092,bigdata03:9092',
        |  'format' = 'json',
        |  'sink.partitioner' = 'default'
        |)
        |""".stripMargin
    tEnv.executeSql(outTableSql)

    //业务逻辑
    //注意：这个SQL语句执行会报错，提示目标表不支持更新
    val execSql =
      """
        |INSERT INTO kafka_sink
        |SELECT
        |  age,
        |  COUNT(*) AS cnt
        |FROM kafka_source
        |GROUP BY age
        |""".stripMargin
    tEnv.executeSql(execSql)
  }

}
