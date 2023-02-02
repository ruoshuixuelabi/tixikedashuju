package com.imooc.scala.state

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction

import scala.collection.mutable.ListBuffer

/**
 * 自定义批量输出Sink
 * Created by xuwei
 */
class MyBufferSink extends SinkFunction[(String,Int)] with CheckpointedFunction{

  //声明一个ListState类型的状态变量
  private var checkpointedState: ListState[(String,Int)] = _

  //定义一个本地缓存
  private val bufferElements = ListBuffer[(String,Int)]()
  /**
   * Sink的核心处理逻辑，将接收到的数据输出到外部系统
   * 接收到一条数据，这个方法就会执行一次
   * @param value
   * @param context
   */
  override def invoke(value: (String, Int), context: SinkFunction.Context): Unit = {
    //将接收到的数据保存到本地缓存中
    bufferElements += value
    //当本地缓存大小达到一定阈值时，将本地缓冲中的数据一次洗输出到外部系统
    if(bufferElements.size == 2){
      println("======start======")
      for(element <- bufferElements){
        println(element)
      }
      println("======end======")
      //清空本地缓冲中的数据
      bufferElements.clear()
    }
  }

  /**
   * 将本地缓存中的数据保存到状态中，在支持checkpoint时，会将状态中的数据持久化到外部存储中
   * @param context
   */
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    //将上次写入到状态中的数据清空
    checkpointedState.clear()
    //将最新的本地缓存中的数据写入到状态中
    for(element <- bufferElements){
      checkpointedState.add(element)
    }
  }

  /**
   * 初始化或者恢复状态
   * @param context
   */
  override def initializeState(context: FunctionInitializationContext): Unit = {
    //注册状态
    val descriptor = new ListStateDescriptor[(String,Int)](
      "buffered-elements",
      classOf[(String,Int)]
    )
    //此时借助于context获取OperatorStateStore，进而获取ListState
    checkpointedState = context.getOperatorStateStore.getListState(descriptor)

    //如果是重启任务，需要从外部存储中读取状态数据并写入到本地缓存中
    if(context.isRestored){
      checkpointedState.get().forEach(e=>{
        bufferElements += e
      })
    }
  }
}
