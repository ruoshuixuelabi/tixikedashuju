package com.imooc.scala.state

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * 直播间数据统计-MapState
 * Created by xuwei
 */
object KeyedState_VideoDataDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC)

    //数据格式
    //送礼数据：{"type":"gift","uid":"1001","vid":"29901","value":100}
    //关注数据：{"type":"follow","uid":"1001","vid":"29901"}
    //点赞数据：{"type":"like","uid":"1001","vid":"29901"}
    val text = env.socketTextStream("bigdata04", 9001)

    import org.apache.flink.api.scala._
    text.map(line=>{
      val videoJsonData = JSON.parseObject(line)
      val vid = videoJsonData.getString("vid")
      val videoType = videoJsonData.getString("type")
      //也可以使用if语句实现
      videoType match {
        case "gift" =>{
          val value = videoJsonData.getIntValue("value")
          (vid,videoType,value)
        }
        case _ =>(vid,videoType,1)
      }
    }).keyBy(_._1)//注意：后面也可以使用flatmap算子，在这里换一种形式，使用低级API process
      .process(new KeyedProcessFunction[String,(String,String,Int),(String,String,Int)] {
        //声明一个MapState类型的状态变量，存储用户的直播间数据指标
        //MapState中key的值为gift\follow\like，value的值为累加后的结果
        private var videoDataState: MapState[String,Int] = _

        override def open(parameters: Configuration): Unit = {
          //注册状态
          val mapStateDesc = new MapStateDescriptor[String,Int](
            "videoDataState",//指定状态名称
            classOf[String],//指定key的类型
            classOf[Int]//指定value的类型
          )
          videoDataState = getRuntimeContext.getMapState(mapStateDesc)
        }

        override def processElement(value: (String, String, Int),
                                    ctx: KeyedProcessFunction[String, (String, String, Int), (String, String, Int)]#Context,
                                    out: Collector[(String, String, Int)]): Unit = {
          val videoType = value._2
          var num = value._3
          //判断状态中是否有这个数据
          if(videoDataState.contains(videoType)){
            num += videoDataState.get(videoType)
          }
          //更新状态
          videoDataState.put(videoType,num)
          out.collect((value._1,videoType,num))
        }
      }).print()

    env.execute("KeyedState_VideoDataDemo")
  }

}
