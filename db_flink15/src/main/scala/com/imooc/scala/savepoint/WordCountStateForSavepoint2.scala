package com.imooc.scala.savepoint

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * 故障情况2：未手工设置uid，重启时算子并行度发生了变化
 * Created by xuwei
 */
object WordCountStateForSavepoint2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC)

    //开启Checkpoint
    env.enableCheckpointing(1000*10)//为了观察方便，在这里设置为10秒执行一次
    //获取Checkpoint的配置对
    val cpConfig = env.getCheckpointConfig
    //在任务故障和手工停止任务时都会保留之前生成的Checkpoint数据
    cpConfig.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //设置Checkpoint后的状态数据的存储位置
    cpConfig.setCheckpointStorage("hdfs://bigdata01:9000/flink-chk/wordcount")

    val text = env.socketTextStream("bigdata04", 9001)
    import org.apache.flink.api.scala._
    val keyedStream = text.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(_._1)

    keyedStream.map(new RichMapFunction[(String,Int),(String,Int)] {
      //声明一个ValueState类型的状态变量，存储单词出现的总次数
      private var countState: ValueState[Int] = _

      /**
       * 任务初始化的时候这个方法执行一次
       * @param parameters
       */
      override def open(parameters: Configuration): Unit = {
        //注册状态
        val valueStateDesc = new ValueStateDescriptor[Int](
          "countState",//指定状态名称
          classOf[Int]//指定状态中存储的数据类型
        )
        countState = getRuntimeContext.getState(valueStateDesc)
      }

      override def map(value: (String, Int)): (String, Int) = {
        //从状态中获取这个key之前出现的次数
        var lastNum = countState.value()
        val currNum = value._2
        //如果这个key的数据是第一次过来，则将之前出现的次数初始化为0
        if(lastNum == null){
          lastNum = 0
        }
        //汇总出现的次数
        val sum = lastNum+currNum
        //更新状态
        countState.update(sum)
        //返回单词及单词出现的总次数
        (value._1,sum)
      }
    }).uid("vs_map001").print()

    env.execute("WordCountStateForSavepoint2")
  }
}
