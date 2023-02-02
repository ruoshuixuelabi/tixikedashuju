package com.imooc.scala.modules

import org.apache.flink.table.api.{EnvironmentSettings, SqlDialect, TableEnvironment}

/**
 * 在Flink SQL中兼容Hive SQL语法
 * Created by xuwei
 */
object UseHiveDialectSQL {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val settings = EnvironmentSettings
      .newInstance()
      //指定执行模式，支持inBatchMode和inStreamingMode
      .inStreamingMode()
      .build()
    val tEnv = TableEnvironment.create(settings)

    //创建Catalog
    val catalogDDL =
      """
        |CREATE CATALOG myhivecatalog WITH(
        |    'type' = 'hive',
        |    'default-database' = 'default',
        |    'hive-conf-dir' = 'hdfs://bigdata01:9000/hive-conf'
        |)
        |""".stripMargin
    tEnv.executeSql(catalogDDL)

    //进入HiveCatalog和default数据库
    tEnv.executeSql("USE CATALOG myhivecatalog")// 使用DDL
    tEnv.executeSql("USE `default`")// 使用DDL

    //设置使用hive方言【注意：此时必须使用HiveCatalog】
    tEnv.getConfig.setSqlDialect(SqlDialect.HIVE)

    // FlinkSQL建表语句执行会报错
    val flinkSqlDDL =
      """
        |CREATE TABLE IF NOT EXISTS orders_test(
        |    order_id    BIGINT,
        |    price       DECIMAL(10,2),
        |    order_time  TIMESTAMP
        |) WITH (
        |    'connector' = 'datagen',
        |    'rows-per-second' = '1'
        |)
        |""".stripMargin
    //tEnv.executeSql(flinkSqlDDL)


    val hiveSqlDDL =
      """
        |CREATE TABLE IF NOT EXISTS flink_stu(
        |id INT,
        |name STRING
        |)
        |""".stripMargin
    tEnv.executeSql(hiveSqlDDL)
    tEnv.executeSql("SELECT * FROM flink_stu").print()
  }

}
