package com.ch.ml.ctr

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 分析广告关键字的点击率
  */
object KWordsAnalyse {

  def main(args: Array[String]) {


    if (args.length != 3) {
      println("Usage:MakeTags <inputLogBase> <outputPath>  <partition>")
      System.exit(0)
    }
    val Array(inputLogBase, outputPath, pars) = args

    //    val masterId = "local[2]"
    //    val inputLogBase = "F:\\company\\lomark\\平台数据&自动化脚本\\原始数据parquet文件\\2016-10-01_00_p1_invalid.1475254927494.log.20161001012611215.parquet"
    //                val inputLogBase = "F:\\company\\lomark\\DMP-new\\点击率预测数据min"
    //    val outputPath = "F:\\output\\"
    //    val pars = "3"


    val conf = new SparkConf()
    //      .setMaster("local[2]").setAppName("xx")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[CTRBean]))

    val sc = new SparkContext(conf)
    // 实际使用的
    val sqlc = new SQLContext(sc)
    sqlc.setConf("spark.sql.parquet.compression.codec", "snappy")
    sqlc.read.parquet(inputLogBase).repartition(pars.toInt)
      .map(a =>
        CTRBean.buidFormParquet(a)
      )
      .filter { log => log.adorderid > 0 && (log.requestmode == 1 || log.requestmode == 3) }
      .groupBy { case k => k.sessionid } /*.filter(_._2.size >= 3)*/ .map {
      x =>
        var click: CTRBean = null
        // resquetMode == 1 && processNode > 1
        val reqBean = x._2.find(_.requestmode == 1)

        val clkBean = x._2.find { log => log.requestmode == 3 && log.iseffective == 1 }
        if (!clkBean.isEmpty && !reqBean.isEmpty) {
          reqBean.get.requestmode = clkBean.get.requestmode // click
          reqBean.get
        } else if (!reqBean.isEmpty) {
          reqBean.get
        } else {
          null
        }
    }
      .filter(_ != null)
      /*      .map{
               log=>
                 (log.adorderid,log.keywords,log.requestmode,log.iseffective)
             }
             .saveAsTextFile(outputPath)
             */


      //    adorderid, keywords, imp, click, ctr
      .flatMap {
      log =>
        val ad = log.adorderid
        val imp = 1
        val click = if (log.requestmode == 3 /*&& log.iseffective == 1*/) 1 else 0

        log.keywords.trim.split("\\|").filter(!_.isEmpty).map {
          key =>
            (ad, key, imp, click)
        }
    }


      // 使用测试数据使用的:
      /*      sc.textFile(inputLogBase).repartition(pars.toInt).map(x => x.split(",", x.length)).flatMap {
              log =>
                log(1).trim.split("\\|").map {
                  key =>
                    (log(0), key, log(2), log(3))
                }
            }*/

      .groupBy { case (ad, key, _, _) => ad + "_" + key }.map {
      case (k, v) =>
        val imp = v.map { case (_, _, imp, _) => imp.toInt }.sum
        val click = v.map { case (_, _, _, click) => click.toInt }.sum
        val ctr = if (imp == 0) 0 else click / imp.toDouble
        val adk = k.split("_", k.length)
        //            (adk(0) + "," + adk(1) + "," + imp + "," + click + "," + ctr)
        (adk(0), adk(1), imp, click, ctr)
    }
      // sort by ctr desc
      .sortBy(_._5, false, 1)
      .map {
        v => (v._1 + "," + v._2 + "," + v._3 + "," + v._4 + "," + v._5)
      }
      .saveAsTextFile(outputPath)

    /* .foreach {
              x =>
    //            println("" + x._1 + "," + x._2 + "," + x._3 + "," + x._4)
            }*/


  }
}
