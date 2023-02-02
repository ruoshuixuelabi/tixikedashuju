package com.imooc.scala.kafkaconnector

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.kafka.clients.consumer.OffsetResetStrategy

/**
 * Flink使用新的消费API从Kafka中读取数据
 *
 * 指定消费者的事务隔离级别
 * Created by xuwei
 */
object FlinkKafkaSourceReadLevelDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC)

    //创建KafkaSource数据源
    val kafkaSource = KafkaSource
      .builder[String]
      .setBootstrapServers("bigdata01:9092,bigdata02:9092,bigdata03:9092")
      .setTopics("dt002")
      //指定消费者组ID
      .setGroupId("gid_dt002")
      .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .setProperty("partition.discovery.interval.ms","10000")//10秒触发一次
      .setProperty("commit.offsets.on.checkpoint","true")
      //指定消费者的事务隔离级别
      .setProperty("isolation.level","read_committed")
      .build

    import org.apache.flink.api.scala._
    val text = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource")

    text.print()

    env.execute("FlinkKafkaSourceReadLevelDemo")
  }

}
