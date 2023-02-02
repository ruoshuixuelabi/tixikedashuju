package com.imooc.scala.ttl

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.StateTtlConfig.TtlTimeCharacteristic
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * State TTL的使用
 * Created by xuwei
 */
object StateTTLDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC)

    //开启Checkpoint
    env.enableCheckpointing(1000*10)//为了观察方便，在这里设置为10秒执行一次


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
        //设置TTL机制相关配置
        val ttlConfig = StateTtlConfig
          //1：指定状态的生存时间
          .newBuilder(Time.seconds(10))
          //2：指定什么时候触发更新延长状态的TTL时间
          //OnCreateAndWrite：仅在创建和写入时触发TTL时间更新延长
          //OnReadAndWrite：表示在读取的时候也会触发(包括创建和写入)
          .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
          //3：过期数据是否可访问
          //NeverReturnExpired：表示永远不会返回过期数据(可能会存在数据已过期但是还没有被清理)
          //ReturnExpiredIfNotCleanedUp：表示数据只要没有被删除，就算过期了也可以被访问
          .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
          //4：TTL的时间语义
          .setTtlTimeCharacteristic(TtlTimeCharacteristic.ProcessingTime)
          //5：过期数据删除策略
          //cleanupFullSnapshot：全量删除
          //此时只有当任务从checkpoint或者savepoint恢复时才会产生所有过期数据
          //这种方式其实并不能真正解决使用HashMapStateBackend时的内存压力问题，只有定时重启恢复才可以解决
          //注意：这种方式不适合Rocksdb中的增量Checkpoint方式
          //.cleanupFullSnapshot()
          //cleanupIncrementally：针对内存的增量删除方式
          //增量删除策略只支持基于内存的HashMapStateBackend，不支持EmbeddedRocksDBStateBackend
          //它的实现思路是在所有数据上维护一个全局迭代器。当遇到某些事件(例如状态访问)时会触发增量删除
          //cleanupSize=100 和 runCleanupForEveryRecord=true 表示每访问一个状态数据就会向前迭代遍历100条数据并删除其中过期的数据
          .cleanupIncrementally(100,true)
          //针对Rocksdb的增量删除方式
          //当Rocksdb在做Compact（合并）的时候删除过期数据
          //每Compact（合并）1000个Entry之后，会从FLink中查询当前时间戳，用于判断这些数据是否过期
          //.cleanupInRocksdbCompactFilter(1000)
          .build()
        //注册状态
        val valueStateDesc = new ValueStateDescriptor[Int](
          "countState",//指定状态名称
          classOf[Int]//指定状态中存储的数据类型
        )
        //开启TTL机制
        //注意：开启TTL机制会增加状态的存储空间，因为在存储状态的时候还需要将状态的上次修改时间一起存储
        valueStateDesc.enableTimeToLive(ttlConfig)
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
    }).print()

    env.execute("StateTTLDemo")
  }

}
