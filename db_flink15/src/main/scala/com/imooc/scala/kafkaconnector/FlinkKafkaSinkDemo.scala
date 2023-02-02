package com.imooc.scala.kafkaconnector

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * Flink使用新的生产API向Kafka中写入数据
 * Created by xuwei
 */
object FlinkKafkaSinkDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC)

    //开启Checkpoint
    env.enableCheckpointing(5000)

    val text = env.socketTextStream("bigdata04", 9001)

    //创建KafkaSink目的地
    val kafkaSink = KafkaSink
        //[String]：指定Kafka中数据(Value)的类型
        .builder[String]
        //指定Kafka集群地址
        .setBootstrapServers("bigdata01:9092,bigdata02:9092,bigdata03:9092")
        //指定序列化器，将数据流中的元素转换为Kafka需要的数据格式
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic("t1")
            .setValueSerializationSchema(new SimpleStringSchema())
            .build()
        )
        //指定KafkaSink提供的容错机制AT_LEAST_ONCE 或者 EXACTLY_ONCE
        .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
        .build

    //注意：需要使用sinkTo，不能使用addSink
    text.sinkTo(kafkaSink)

    env.execute("FlinkKafkaSinkDemo")
  }

}
