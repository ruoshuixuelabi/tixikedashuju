package com.imooc.scala.streamencode

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Schema
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.connector.ChangelogMode

/**
 * Append-only 数据流
 * Created by xuwei
 */
object AppendOnlySQL {
  def main(args: Array[String]): Unit = {
    //由于需要将Table转为DataStream，所以需要使用StreamTableEnviroment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    //设置全局默认并行度
    env.setParallelism(1)

    //创建输入表
    val inTableSql =
      """
        |CREATE TABLE orders(
        |    order_id    BIGINT,
        |    price       DECIMAL(10,2),
        |    order_time  TIMESTAMP
        |) WITH (
        |    'connector' = 'datagen',
        |    'rows-per-second' = '1'
        |)
        |""".stripMargin
    tEnv.executeSql(inTableSql)

    //执行SQL查询操作
    val resTable = tEnv.sqlQuery("SELECT * FROM orders")

    //将结果转换为DataStream数据流
    val resStream = tEnv.toChangelogStream(resTable,
      Schema.newBuilder().build(),
      ChangelogMode.insertOnly()
    )

    //打印DataStream数据流中的数据
    resStream.print()

    //执行
    env.execute("AppendOnlySQL")
  }

}
