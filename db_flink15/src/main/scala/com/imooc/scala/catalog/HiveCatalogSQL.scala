package com.imooc.scala.catalog

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.catalog.hive.HiveCatalog

/**
 * 使用HiveCatalog存储Flink SQL的元数据
 * Created by xuwei
 */
object HiveCatalogSQL {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val settings = EnvironmentSettings
      .newInstance()
      //指定执行模式，支持inBatchMode和inStreamingMode
      .inStreamingMode()
      .build()
    val tEnv = TableEnvironment.create(settings)

    //创建Catalog，可以使用两种方式
    //第一种方式：使用API
    //useCatalogApi(tEnv)

    //第二种方式：使用DDL
    useCatalogDDL(tEnv)

    //使用Catalog
    tEnv.useCatalog("myhivecatalog")// 使用API
    //tEnv.executeSql("USE CATALOG myhivecatalog")// 使用DDL

    //使用Database
    tEnv.useDatabase("default")// 使用API
    //注意：使用默认的default数据库的时候，需要给default增加反引号转义标识，否则SQL语法解析报错，因为default属于保留关键字
    //tEnv.executeSql("USE `default`")// 使用DDL

    //创建表
    //注意：此时这个表的元数据信息会存储到HiveCatalog中指定的Hive Metastore中。
    val inTableSql =
      """
        |-- 增加了IF NOT EXISTS之后可以多次执行此SQL语句，只有表不存在时才会真正执行
        |CREATE TABLE IF NOT EXISTS orders(
        |    order_id    BIGINT,
        |    price       DECIMAL(10,2),
        |    order_time  TIMESTAMP
        |) WITH (
        |    'connector' = 'datagen',
        |    'rows-per-second' = '1'
        |)
        |""".stripMargin
    tEnv.executeSql(inTableSql)


    //查询表中的数据【此时也可以直接查询Hive中的表】
    tEnv.executeSql("SELECT * FROM orders").print()

    //删除表
    //tEnv.executeSql("DROP TABLE orders")
  }

  /**
   * 使用API创建Catalog
   * @param tEnv
   */
  def useCatalogApi(tEnv :TableEnvironment): Unit ={
    //catalogName这个参数必须要指定，参数名称可以随机指定，因为最终在Hive的Metastore中不存储catalogName
    val catalogName = "myhivecatalog"
    //defaultDatabase这个参数必须指定Hive中存在的数据库名称
    val defaultDatabase = "default"
    //hiveConfDir这个参数可以不指定，不指定的时候Flink默认会到本地类路径下（resources）查找hive-site.xml
    //hiveConfDir这个参数也可以指定具体的目录，建议使用HDFS，在HDFS目录下需要有hive-site.xml
    val hiveConfDir = "hdfs://bigdata01:9000/hive-conf"
    val hive = new HiveCatalog(catalogName,defaultDatabase,hiveConfDir)
    //创建Catalog
    tEnv.registerCatalog(catalogName,hive)
  }

  /**
   * 使用DDL创建Catalog
   * @param tEnv
   */
  def useCatalogDDL(tEnv :TableEnvironment): Unit ={
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
  }

}
