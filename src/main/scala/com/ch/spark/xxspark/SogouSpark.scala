package com.ch.spark.xxspark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * xiao xiang spark
  * 利用sogou日志数据
  * 统计pv 和 uv
  */
object SogouSpark {

  def main(args: Array[String]) {
    if (args.length != 2) {
      println("Usage:MakeTags <inputLogBase> <outputPath> ")
//      System.exit(0)
    }
//    val Array(inputLogBase, outputPath) = args

    val inputLogBase = "src/main/scala/com/ch/spark/xxspark/data/SogouQ.reduced.utf8.10000"
    val outputPath = "src/main/scala/com/ch/spark/xxspark/data2"
    val conf = new SparkConf()
      .setAppName("SoGou").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile(inputLogBase)
      .map { x =>
        val arr = x.split("\t")
        val hour = arr(0).split(":")(0)
        val uid = arr(1)
        hour + "\t" + uid
      }.map(_.split("\t").toList).cache()

    val rddpv = rdd1.map(x => (x(0), 1)).reduceByKey(_ + _)
    /*flatMap(_(0).split("\t")).map((_,1)) => map(x=>(x(0),1))*/
    val rdduv = rdd1.distinct().map(x => (x(0), 1)).reduceByKey(_ + _)
    // sortByKey()  排序
    rddpv.join(rdduv).coalesce(1).sortBy(_._1). map(x => ("[" + x._1 + "],[" + x._2._1 + "],[" + x._2._2 + "]"))
      .saveAsTextFile(outputPath)


    val rdd = sc.textFile(inputLogBase).map{
      x =>
        val arr = x.split("\t")
        val hour = arr(0).split(":")(0)
        val uid = arr(1)
        (hour,uid)
    }.cache()
    val pv = rdd.map(x=>(x._1,1)).reduceByKey(_ + _)
    val uv = rdd.distinct().map(x=>(x._1,1)).reduceByKey(_ + _)
    pv.join(uv).coalesce(1).sortBy(_._1).map(x=>(x._1,x._2._1,x._2._2))
      .foreach(println)

  }
}


