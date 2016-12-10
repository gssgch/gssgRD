package com.ch.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ch on 2016/12/10.
  * Broadcast va
  */
object BroadcastDemo {
  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setMaster("local[2]").setAppName("xx")

    val sc = new SparkContext(conf)

     val bcData = sc.broadcast(Array("test"))

    // 不写/ 就是相对路径，从项目根路径开始
    sc.textFile("src/main/scala/com/ch/spark/xxspark/data/SogouQ.reduced.utf8.10000")
      .foreach{
        x=>
        val bcArray = bcData.value
        println(bcArray)
      }


//    print(broadcastData)
  }
}
