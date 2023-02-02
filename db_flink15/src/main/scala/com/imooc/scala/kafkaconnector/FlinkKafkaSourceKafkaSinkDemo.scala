package com.imooc.scala.kafkaconnector

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.kafka.clients.consumer.OffsetResetStrategy

/**
 * Kafka+Flink+Kafka实现端到端一致性语义
 * Created by xuwei
 */
object FlinkKafkaSourceKafkaSinkDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC)

    //开启Checkpoint
    env.enableCheckpointing(5000*2)

    //创建KafkaSource数据源
    val kafkaSource = KafkaSource
      .builder[String]
      .setBootstrapServers("bigdata01:9092,bigdata02:9092,bigdata03:9092")
      .setTopics("dt001")
      .setGroupId("gid_dt001")
      .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .setProperty("partition.discovery.interval.ms","10000")//10秒触发一次
      .setProperty("commit.offsets.on.checkpoint","true")
      .build

    import org.apache.flink.api.scala._
    val text = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource")

    //创建KafkaSink目的地
    val kafkaSink = KafkaSink
      .builder[String]
      .setBootstrapServers("bigdata01:9092,bigdata02:9092,bigdata03:9092")
      .setRecordSerializer(KafkaRecordSerializationSchema.builder()
        .setTopic("dt002")
        .setValueSerializationSchema(new SimpleStringSchema())
        .build()
      )
      .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
      .build

    text.map(line=>{
      line
    }).sinkTo(kafkaSink)

    env.execute("FlinkKafkaSourceKafkaSinkDemo")
  }

}
