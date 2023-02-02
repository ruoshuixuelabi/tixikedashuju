package com.imooc.scala.catalog

import com.imooc.scala.catalog.HiveCatalogSQL.useCatalogDDL
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/**
 * 使用完整的表名
 * Created by xuwei
 */
object HiveCatalogUseFullNameSQL {
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

    //创建Database
    tEnv.executeSql("CREATE DATABASE IF NOT EXISTS myhivecatalog.flinkdb")

    //创建表
    //注意：此时这个表的元数据信息会存储到HiveCatalog中指定的Hive Metastore中。
    val inTableSql =
    """
      |-- 增加了IF NOT EXISTS之后可以多次执行此SQL语句，只有表不存在时才会真正执行
      |CREATE TABLE IF NOT EXISTS myhivecatalog.flinkdb.orders_bak(
      |    order_id    BIGINT,
      |    price       DECIMAL(10,2),
      |    order_time  TIMESTAMP
      |) WITH (
      |    'connector' = 'datagen',
      |    'rows-per-second' = '1'
      |)
      |""".stripMargin
    tEnv.executeSql(inTableSql)

    //查询表中的数据
    //tEnv.executeSql("SELECT * FROM myhivecatalog.flinkdb.orders_bak").print()
    //注意：使用默认的default数据库的时候，需要给default增加反引号转义标识，否则SQL语法解析报错，因为default属于保留关键字
    //tEnv.executeSql("SELECT * FROM myhivecatalog.`default`.orders").print()

    //删除表
    //tEnv.executeSql("DROP TABLE myhivecatalog.flinkdb.orders_bak")
  }

}
