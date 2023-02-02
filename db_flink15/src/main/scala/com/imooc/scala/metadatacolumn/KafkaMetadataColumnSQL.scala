package com.imooc.scala.metadatacolumn

import java.time.ZoneId

import org.apache.flink.configuration.CoreOptions
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/**
 * 使用Kafka中的元数据列
 * Created by xuwei
 */
object KafkaMetadataColumnSQL {
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
        |CREATE TABLE kafka_source(
        |  name STRING,
        |  age INT,
        |  `topic` STRING METADATA VIRTUAL,
        |  `partition` INT METADATA VIRTUAL,
        |  `headers` MAP<STRING,STRING> METADATA,
        |  `leader-epoch` INT METADATA VIRTUAL,
        |  `offset` BIGINT METADATA VIRTUAL,
        |  k_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
        |  `timestamp-type` STRING METADATA VIRTUAL
        |)WITH(
        |  'connector' = 'kafka',
        |  'topic' = 'dt001',
        |  'properties.bootstrap.servers' = 'bigdata01:9092,bigdata02:9092,bigdata03:9092',
        |  'properties.group.id' = 'gid-sql-2',
        |  'scan.startup.mode' = 'group-offsets',
        |  'properties.auto.offset.reset' = 'latest',
        |  'format' = 'json',
        |  'json.fail-on-missing-field' = 'false',
        |  'json.ignore-parse-errors' = 'true'
        |)
        |""".stripMargin
    tEnv.executeSql(inTableSql)

    //查看输入表中的数据
    val querySQL =
      """
        |SELECT
        |  name,
        |  age,
        |  `topic`,
        |  `partition`,
        |  `headers`,
        |  `leader-epoch`,
        |  `offset`,
        |  k_timestamp,
        |  `timestamp-type`
        |FROM kafka_source
        |""".stripMargin
    tEnv.executeSql(querySQL).print()
  }

}
