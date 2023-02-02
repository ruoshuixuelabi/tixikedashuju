package com.imooc.scala.checkpoint

import org.apache.flink.util.MathUtils

/**
 * 测试默认算子最大并行度
 * 结论：Flink生成的算子最大并行度介于128和32768之间
 * Created by xuwei
 */
object TestDefaultMaxParallelism {
  def main(args: Array[String]): Unit = {
    //这个参数表示当前算子(任务)的并行度
    val operatorParallelism = 100
    //最小值：2的7次方=128
    val DEFAULT_LOWER_BOUND_MAX_PARALLELISM: Int = 1 << 7
    //最大值：2的15次方=32768
    val UPPER_BOUND_MAX_PARALLELISM: Int = 1 << 15

    //计算算子最大并行度
    val res = Math.min(
      Math.max(
        MathUtils.roundUpToPowerOfTwo(
          operatorParallelism + (operatorParallelism / 2)),
        DEFAULT_LOWER_BOUND_MAX_PARALLELISM),
      UPPER_BOUND_MAX_PARALLELISM)
    println(res)
  }

}
