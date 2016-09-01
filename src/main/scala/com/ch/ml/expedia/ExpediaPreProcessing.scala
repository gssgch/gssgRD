package com.ch.ml.expedia

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.util.MLUtils

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Created by ch on 2016/8/25.
  */
object ExpediaPreProcessing {

  def main(args: Array[String]) {
    // 在终端商过滤无关日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    if(args.length!=2){
      println("Usage:ExpediaTrain <filePath> <fileTpye> <outputPath>")
      System.exit(0)
    }
    val Array(filePath,fileType,outputPath) = args

    // 设置参数
    val conf = new SparkConf().setAppName("expedia").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //求列数
    //    data_train.map(x => println(x.size))

    // 数据预处理 并存文件
  dataPreProcessing(sc,filePath,fileType).saveAsTextFile(outputPath)


    // 训练模型


    val data = MLUtils.loadLibSVMFile(sc, "expedia_ml/part-00000")
    //     val testData =MLUtils.loadLibSVMFile(sc, "expedia_ml_test/part-00000")
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))
    /*
        data.foreach{
          case(k,v)=>
            println(k+"\nv="+v)
        }
        val dataTest1= data5date.map {
          x =>
            val a = new ArrayBuffer[String]
    //        a += x.last
            for (i <- 0 to x.length - 1) {
              a += i + ":" + x(i)
            }
            (x.last,a.toArray.mkString(" "))
        }*/

    // 训练GBDT模型
    val boostingStrategy = BoostingStrategy.defaultParams("Classification").setNumIterations(3)

    //    val model = GradientBoostedTrees.train(trainingData, boostingStrategy)

    // 计算测试误差
    /*   val testErr = testData.map { point => val prediction = model.predict(point.features)
         if (point.label == prediction) 1.0 else 0.0
       }.mean()
       println("Test Error = " + testErr)
       println("Learned GBT model:n" + model.toDebugString)*/

    // 0.9720347763749159

    //计算泛化误差
    /*  val testErr = testData.map { point => val prediction = model.predict(point.features)
        if (point.label == prediction) 1.0 else 0.0
      }.coalesce(1,true).saveAsTextFile("hdfs:///user/s-56/output")
      println("Test Error = " + testErr)
      println("Learned GBT model:n" + model.toDebugString)*/


    /*
         val fractions = data.map(_._1).distinct.map(x => (x,0.8)).collectAsMap
         val sampleData = data.sampleByKey(withReplacement = false,fractions)

             // 训练GBDT模型
             val boostingStrategy = BoostingStrategy.defaultParams("Classification")
             boostingStrategy.numIterations = 3 // Note: Use more in practice
             val model = GradientBoostedTrees.train(sampleData, boostingStrategy)

             // 计算测试误差
             val testErr = testData.map { point => val prediction = model.predict(point.features)
               if (point.label == prediction) 1.0 else 0.0
             }.mean()
             println("Test Error = " + testErr)
             println("Learned GBT model:n" + model.toDebugString)

             // 0.9720347763749159

             //计算泛化误差
             val testErr = testData.map { point => val prediction = model.predict(point.features)
               if (point.label == prediction) 1.0 else 0.0
             }.coalesce(1,true).saveAsTextFile("hdfs:///user/s-56/output")
             println("Test Error = " + testErr)
             println("Learned GBT model:n" + model.toDebugString)  */

  }


  /**
    * 数据预处理 把csv文件格式转换为libsvm 文件格式
    *
    * @param fileName
    * @param fileType
    */
  def dataPreProcessing(sc: SparkContext, fileName: String, fileType: String) = {
    //修改train数据 / test
    val data_train = sc.textFile(fileName)
      .filter(!_.contains("srch_id")).map{_.replace("NULL", "0").split(",")}
    //删除日期列:
    val data5date = data_train.map {
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
