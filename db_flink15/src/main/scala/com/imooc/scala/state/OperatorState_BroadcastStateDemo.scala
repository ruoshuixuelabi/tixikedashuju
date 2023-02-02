package com.imooc.scala.state

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

import scala.collection.JavaConverters.mapAsJavaMap
import scala.collection.mutable

/**
 * BroadcastState在两个流连接中的应用（双流Join）
 * 这个场景类似于：一个事实表（事件数据流） left join 一个维度表 （配置数据流）
 * Created by xuwei
 */
object OperatorState_BroadcastStateDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC)

    import org.apache.flink.api.scala._
    //构建第1个数据流：事件数据流
    val eventStream = env.addSource(new MyStreamSource)

    //构建第2个数据流：配置数据流
    val confStream = env.addSource(new MyRedisSource)

    //将配置数据流广播出去，变成广播数据流，并且注册一个MapState
    val countryAreaMapStateDescriptor = new MapStateDescriptor[String,String](
      "countryArea",
      classOf[String],
      classOf[String]
    )
    val broadcastStream = confStream.broadcast(countryAreaMapStateDescriptor)

    //将两个流进行连接
    val broadcastConnectStream = eventStream.connect(broadcastStream)

    //处理连接后的流
    broadcastConnectStream.process(new BroadcastProcessFunction[String,mutable.Map[String,String],String] {
      //处理事件数据流中的数据
      override def processElement(value: String,
                                  ctx: BroadcastProcessFunction[String, mutable.Map[String, String], String]#ReadOnlyContext,
                                  out: Collector[String]): Unit = {
        val jsonObj = JSON.parseObject(value)
        val countryCode = jsonObj.getString("countryCode")
        //取出广播状态中的数据
        val broadcastState = ctx.getBroadcastState(countryAreaMapStateDescriptor)
        val area = broadcastState.get(countryCode)
        if(area!=null){
          jsonObj.put("countryCode",area)
          out.collect(jsonObj.toJSONString)
        }
      }
      //处理广播后的配置数据流中的数据
      override def processBroadcastElement(value: mutable.Map[String, String],
                                           ctx: BroadcastProcessFunction[String, mutable.Map[String, String], String]#Context,
                                           out: Collector[String]): Unit = {
        //获取BroadcastState
        val broadcastState = ctx.getBroadcastState(countryAreaMapStateDescriptor)
        //清空BroadcastState中的数据
        broadcastState.clear()
        //重新写入最新的映射关系数据
        broadcastState.putAll(mapAsJavaMap(value))
      }
    }).print()

    env.execute("OperatorState_BroadcastStateDemo")
  }
}
