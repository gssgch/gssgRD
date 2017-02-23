package com.ch.spark.ml.ctr

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * 点击率预测
  * v1 预处理之后，train和test分开
  */
object PreProcessing {


  //input (1fbe01fe,f3845767,28905ebd,ecad2386,7801e8d9)
  //output ((0:1fbe01fe),(1:f3845767),(2:28905ebd),(3:ecad2386),(4:7801e8d9))
  def parseCatFeatures(catfeatures: Array[String]): List[(Int, String)] = {
    val catfeatureList = new ListBuffer[(Int, String)]()
    for (i <- 0 until catfeatures.length) {
      catfeatureList += i -> catfeatures(i).toString
    }
    catfeatureList.toList
  }

  def main(args: Array[String]) {


    if (args.length != 4) {
      println("Usage:MakeTags <inputLogBase> <outputPath1> <partition>")
      System.exit(0)
    }
    val Array(inputLogBase, outputPath, pars) = args

    //            val masterId = "local[2]"
    //            val inputLogBase = "F:\\company\\lomark\\平台数据&自动化脚本\\原始数据parquet文件\\2016-10-01_00_p1_invalid.1475254927494.log.20161001012611215.parquet"
    //            val outputPath = "F:\\output\\"
    //            val pars = "3"


    val conf = new SparkConf()
    //              .setMaster("local[2]").setAppName("xx")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[CTRBean]))

    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)
    sqlc.setConf("spark.sql.parquet.compression.codec", "snappy")

    val data = sqlc.read.parquet(inputLogBase)
      .coalesce(pars.toInt) // repartition
      .map(a =>
      CTRBean.buidFormParquet(a)
    )
      .filter { log => log.adorderid > 0 && (log.requestmode == 2 || log.requestmode == 3) }
      .groupBy { case k => k.sessionid }.map {
      x =>
        val reqBean = x._2.find(_.requestmode == 2)
        val clkBean = x._2.find { log => log.requestmode == 3 && log.iseffective == 1 }
        if (!clkBean.isEmpty && !reqBean.isEmpty) {
          reqBean.get.requestmode = clkBean.get.requestmode // click
          reqBean.get.sessionid = "1" // label  click
          reqBean.get.toString
        } else if (!reqBean.isEmpty /*&& math.random>0.7*/ ) {
          reqBean.get.sessionid = "0"
          //          println("xxxx===",reqBean.get.sdkversionnumber,reqBean.get.adplatformkey,reqBean.get.title,reqBean.get.keywords)
          reqBean.get.toString
        } else {
          null
        }
    }
      .filter(_ != null)
      .coalesce(1).saveAsTextFile(outputPath)
    /*
     // 还可以使用reduceBykey 数据格式还是有点问题
    .map(x => (x.sessionid, x)).reduceByKey {
    (a, b) =>
      val reqBean = if (a.requestmode == 2) a else b
      val clkBean = if (a.requestmode == 3 && a.iseffective == 1) a else if (b.requestmode == 3 && b.iseffective == 1) b
      if (clkBean.isInstanceOf[CTRBean]) {
        reqBean.requestmode = clkBean.asInstanceOf[CTRBean].requestmode // click
        reqBean.sessionid = "1" // label  click
        //          reqBean.toString
      } else {
        reqBean.sessionid = "0"
        //          reqBean.toString
      }
      reqBean
  }
    .map{
      x=>x._2.toString
    }
    */


    //    println("len==="+data.take(1).clone().size)
    //      data.take(50).foreach(println)

    /*
        val oheMap = data.map {
          x =>
            val log = x.split(",",-1) // log.length = 28
              parseCatFeatures(log.slice(24,log.size-1)) // 后4列需要one of encoding
    //        parseCatFeatures(Array(log.sdkversionnumber, log.adplatformkey, log.title, log.keywords))
        }.flatMap(x => x).distinct().zipWithIndex().collectAsMap()

        val preData=data.map {
          log =>
            val a = new ArrayBuffer[String]

            val cat_features_indexed = parseCatFeatures(Array(log.sdkversionnumber, log.adplatformkey, log.title, log.keywords))
            a += log.sessionid // label
          val ctrBean = log.toString.split(",", -1)
            // 去除了后4个 o hot encoding
            for (i <- 1 until ctrBean.length - 4) {
              try {
                a += i + ":" + (if (ctrBean(i).isEmpty) "0" else ctrBean(i))
              } catch {
                case e: java.lang.Exception =>
                case _ =>
              }
            }

            for (k <- cat_features_indexed) {
              if (oheMap contains k) {
                a += (ctrBean.length - 4 + k._1) + ":" + (oheMap get (k)).get.toDouble
              } else {
                a += (ctrBean.length - 4 + k._1) + ":" + 0.0
              }
            }
            a.toArray.mkString(" ")
        }


    //    println("updata=-----")
    //    preData.take(50).foreach(println)

        val splits = preData.randomSplit(Array(0.7, 0.3))
        val (trainData, testData) = (splits(0), splits(1))

        trainData.coalesce(1, true).saveAsTextFile(outputPath)
        testData.coalesce(1, true).saveAsTextFile(outputPath2)*/
  }
}
