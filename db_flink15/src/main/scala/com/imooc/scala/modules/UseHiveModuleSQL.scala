package com.imooc.scala.modules

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.module.hive.HiveModule

/**
 * 在Flink SQL中兼容Hive SQL函数
 * Created by xuwei
 */
object UseHiveModuleSQL {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val settings = EnvironmentSettings
      .newInstance()
      //指定执行模式，支持inBatchMode和inStreamingMode
      .inStreamingMode()
      .build()
    val tEnv = TableEnvironment.create(settings)

    //查看目前可以使用的Module
    tEnv.executeSql("SHOW FULL MODULES").print()

    //加载Hive Module  使用API
    tEnv.loadModule("hive",new HiveModule("3.1.2"))
    //加载Hive Module  使用DDL
    //tEnv.executeSql("LOAD MODULE hive WITH('hive-version' = '3.1.2')")

    //查看目前可以使用的Module
    tEnv.executeSql("SHOW FULL MODULES").print()

    //使用Hive Module，禁用Core Module
    //tEnv.executeSql("USE MODULES hive")

    //使用Core Module，禁用Hive Module
    //tEnv.executeSql("USE MODULES core")

    //使用Hive Module和Core Module，改变加载Module的顺序
    //tEnv.executeSql("USE MODULES hive,core")

    //卸载Hive Module
    //tEnv.executeSql("UNLOAD MODULE hive")

    //GET_JSON_OBJECT是Hive SQL中支持的函数，Flink SQL中是不支持的
    val execSql =
      """
        |SELECT
        |GET_JSON_OBJECT('{"name":"tom","age":20}','$.name') AS name
        |""".stripMargin
    tEnv.executeSql(execSql).print()
  }

}
