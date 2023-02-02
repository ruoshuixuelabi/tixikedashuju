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
 * Created by xuwei
 */
object FlinkKafkaSourceDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC)

    //创建KafkaSource数据源
    val kafkaSource = KafkaSource
      //[String]：指定Kafka中数据(Value)的类型
      .builder[String]
      //指定Kafka集群地址
      .setBootstrapServers("bigdata01:9092,bigdata02:9092,bigdata03:9092")
      //指定需要消费的Topic名称，可以指定多个
      //也可以通过.setTopicPattern("data*")指定一个规则，自动匹配满足条件的Topic
      .setTopics("t1")
      //指定消费者组ID
      .setGroupId("gid_001")
      //设置消费策略
      //committedOffsets()：表示从之前消费者提交的偏移量继续消费数据，如果找不到，则报错
      //committedOffsets(OffsetResetStrategy.EARLIEST)：表示默认从之前消费者提交的偏移量继续消费数据，如果找不到，则按照OffsetResetStrategy指定的策略
      //committedOffsets(OffsetResetStrategy.LATEST)：同上
      //其实committedOffsets()等于committedOffsets(OffsetResetStrategy.NONE)
      //latest()：从最新的数据开始消费
      //earliest()：从最早的数据开始消费
      .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
      //指定Kafka中数据(Value)的序列/反序列化类
      .setValueOnlyDeserializer(new SimpleStringSchema())
      //注意：KafkaSource可以支持批处理模式和流处理模式(默认)
      //.setBounded(..)：这个方法可以设置停止偏移量，结合批处理模式，当消费到停止偏移量时，数据源会退出
      //.setUnbounded()：这个方法可以设置停止偏移量，结合流处理模式，当消费到停止偏移量时，数据源会退出
      //开启动态分区发现机制(默认禁用)，并且设置发现的时机，这样可以在不重启作业的情况下发现Topic中新增的分区
      .setProperty("partition.discovery.interval.ms","10000")//10秒触发一次
      //指定是否在Checkpoint执行成功后向Kafka中提交消费者偏移量，默认为true，表示开启
      //在checkpoint的时候同时在Kafka中维护消费者偏移量是为了对外暴露消费者的消费进度，便于监控，不依赖于它实现消费者的容错
      .setProperty("commit.offsets.on.checkpoint","true")
      //构建KafkaSource
      .build

    import org.apache.flink.api.scala._
    //注意：需要使用fromSource，不能使用addSource
    val text = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource")

    text.print()

    env.execute("FlinkKafkaSourceDemo")
  }

}
