package com.imooc.scala.state

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * 订单数据补全-ListState（双流Join）
 * 订单数据流 + 支付数据流
 * Created by xuwei
 */
object KeyedState_OrderDataDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC)

    //数据格式为
    //订单数据流：{"pid":"1001","pname":"n1"}
    //支付数据流：{"pid":"1001","pstatus":"success"}
    val orderText = env.socketTextStream("bigdata04", 9001)
    val payText = env.socketTextStream("bigdata04", 9002)

    import org.apache.flink.api.scala._
    //解析订单数据流
    val orderTupleData = orderText.map(line => {
      val orderJsonObj = JSON.parseObject(line)
      val pid = orderJsonObj.getString("pid")
      val pname = orderJsonObj.getString("pname")
      (pid, pname)
    })

    //解析支付数据流
    val payTupleData = payText.map(line => {
      val payJsonObj = JSON.parseObject(line)
      val pid = payJsonObj.getString("pid")
      val pstatus = payJsonObj.getString("pstatus")
      (pid, pstatus)
    })

    //针对两个流进行分组+connect连接(也可以先对两个流分别调用keyBy，再调用connect，效果一样)
    orderTupleData.connect(payTupleData)
      .keyBy("_1","_1")//field1表示第1个流里面的分组字段，field2表示第2个流里面的分组字段
      .process(new KeyedCoProcessFunction[String,(String,String),(String,String),(String,String,String)] {
        //声明两个ListState类型的状态变量，分别存储订单数据流和支付数据流
        /**
         * 注意：针对这个业务需求，pid在一个数据流中是不会重复的，其实使用ValueState也是可以的，
         * 因为在这里已经通过keyBy基于pid对数据分组了，所以只需要在状态中存储pname或者pstatus即可。
         * 但是如果pid数据在一个数据流里面会重复，那么就需要要使用ListState，这样才能存储针对pid的多条数据
         */
        private var orderDataState: ListState[(String,String)] = _
        private var payDataState: ListState[(String,String)] = _

        override def open(parameters: Configuration): Unit = {
          //注册状态
          val orderListStateDesc = new ListStateDescriptor[(String,String)](
            "orderDataState",
            classOf[(String,String)]
          )
          val payListStateDesc = new ListStateDescriptor[(String,String)](
            "payDataState",
            classOf[(String,String)]
          )
          orderDataState = getRuntimeContext.getListState(orderListStateDesc)
          payDataState = getRuntimeContext.getListState(payListStateDesc)
        }

        //处理订单数据流
        override def processElement1(orderTup: (String, String),
                                     ctx: KeyedCoProcessFunction[String, (String, String), (String, String), (String, String, String)]#Context,
                                     out: Collector[(String, String, String)]): Unit = {
          //获取当前pid对应的支付数据流，关联之后数据厨具，（可能是支付数据先到）
          payDataState.get().forEach(payTup=>{
            out.collect((orderTup._1,orderTup._2,payTup._2))
          })
          //将本地接收到的订单数据添加到状态中，便于和支付数据流中的数据关联
          orderDataState.add(orderTup)
        }

        //处理支付数据流
        override def processElement2(payTup: (String, String),
                                     ctx: KeyedCoProcessFunction[String, (String, String), (String, String), (String, String, String)]#Context,
                                     out: Collector[(String, String, String)]): Unit = {
          //获取当前pid对应的订单数据流，关联之后输出数据，（可能是订单数据先到）
          orderDataState.get().forEach(orderTup=>{
            out.collect((orderTup._1,orderTup._2,payTup._2))
          })
          //将本地接收到的订单数据添加到状态中，便于和订单数据流中的数据关联
          payDataState.add(payTup)
        }
      }).print()

    env.execute("KeyedState_OrderDataDemo")
  }

}
