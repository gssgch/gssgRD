package com.ch.ml.expedia

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer

/**
  * Created by ch on 2016/8/25.
  */
object ExpediaPreProcessing {

  def main(args: Array[String]) {
    // 在终端商过滤无关日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    if (args.length != 4) {
      println("Usage:ExpediaTrain <trainPath> <testPath> <trainOutPath> <testOutPath>")
//      System.exit(0)
    }

    val args1=Array("file:///I:\\chinahadoop\\机器学习训练营\\训练营作业&代码\\3，推荐系统项目\\train.csv"
    ,"file:///I:\\chinahadoop\\机器学习训练营\\训练营作业&代码\\3，推荐系统项目\\test.csv"
    ,"train"
    ,"test")
    val Array(trainPath, testPath, trainOutPath, testOutPath) = args1

    // 设置参数
    val conf = new SparkConf().setAppName("expedia").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //修改train数据 / test
    val data_train = sc.textFile(trainPath)
    val data_test = sc.textFile(testPath)

    // 数据预处理 并存文件
    dataPreProcessing(data_train,"train").saveAsTextFile(trainOutPath)
    dataPreProcessing(data_test,"test").saveAsTextFile(testOutPath)

    sc.stop()
  }

  /**
    * 数据预处理 把csv文件格式转换为libsvm 文件格式
    *
    * @param fileType ""
    */
  def dataPreProcessing(rdd: RDD[String], fileType: String) = {
    //    数据处理
    val dataRDD = rdd.filter(!_.contains("srch_id")).map {
      _.replace("NULL", "0").split(",")
    }
    //删除日期列:
    val data5date = dataRDD.map {
      x =>
        val x2 = x.toBuffer
        x2.remove(1, 1) // 下标1开始，删除1列
        x2.toArray
    }

    //转换为libsvm格式: lable k1:v1 k2:v2
    val dataLibsvm = data5date.map {
      x =>
        val a = new ArrayBuffer[String]
        a += x.last
        // 正常这里是1，但是train和test数据集的列数不一样，train比test多了后三列，为了predit时正确，就把train数据集后三列删除
        var clnNum = 1
        if (fileType.equals("train")) {
          clnNum = 4
        }
        for (i <- 0 to x.length - clnNum) {
          a += i + ":" + x(i)
        }
        a.toArray.mkString(" ")
    }
    dataLibsvm
  }
}
