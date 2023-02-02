package com.imooc.scala.checkpoint

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * Checkpoint相关配置
 * Created by xuwei
 */
object CheckpointCoreConf {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC)

    //开启Checkpoint，并且指定自动执行的间隔时间
    env.enableCheckpointing(1000*60*2)//2分钟

    //高级选项
    //获取Checkpoint的配置对象
    val cpConfig = env.getCheckpointConfig
    //设置语义模式为EXACTLY_ONCE（默认就是EXACTLY_ONCE）
    cpConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //设置两次Checkpoint之间的最小间隔时间（设置为5秒：表示Checkpoint完成后的5秒内不会开始生成新的Checkpoint）
    cpConfig.setMinPauseBetweenCheckpoints(1000*5)//5秒
    //设置最多运行同时运行几个Checkpoint（默认值是1，也建议使用默认值1，这样可以减少资源占用）
    cpConfig.setMaxConcurrentCheckpoints(1)
    //设置一次Checkpoint的执行超时时间，达到超时时间后会被取消执行，可以避免Checkpoint执行时间过长
    cpConfig.setCheckpointTimeout(1000*60*6)//6分钟
    //设置允许的连续Checkpoint失败次数(默认值是0，表示Checkpoint只要执行失败任务也会立刻失败)
    //偶尔的Checkpoint失败不应该导致任务执行失败，可能是由于一些特殊情况(网络问题)导致Checkpoint失败
    //应该设置一个容错值，如果连续多次Checkpoint失败说明确实是由问题了，此时可以让任务失败
    cpConfig.setTolerableCheckpointFailureNumber(3)
    //设置在手工停止任务时是否保留之前生成的Checkpoint数据（建议使用RETAIN_ON_CANCELLATION）
    //RETAIN_ON_CANCELLATION：在任务故障和手工停止任务时都会保留之前生成的Checkpoint数据
    //DELETE_ON_CANCELLATION：只有在任务故障时才会保留，如果手工停止任务会删除之前生成的Checkpoint数据
    cpConfig.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //设置Checkpoint后的状态数据的存储位置
    //支持JobManagerCheckpointStorage(默认)和FileSystemCheckpointStorage
    //JobManagerCheckpointStorage：表示Checkpoint后的状态数据存储在JobManager节点的JVM堆内存中
    //FileSystemCheckpointStorage：表示Checkpoint后的状态数据存储在文件系统中（可以使用分布式文件系统HDFS）
    //可以简写为：cpConfig.setCheckpointStorage("hdfs://bigdata01:9000/flink-chk001")
    cpConfig.setCheckpointStorage(new FileSystemCheckpointStorage("hdfs://bigdata01:9000/flink-chk001"))



  }

}
