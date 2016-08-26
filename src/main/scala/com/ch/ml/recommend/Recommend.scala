package com.ch.ml.recommend

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd._

import scala.io.Source

/**
  * 使用sparkML实现电影推荐系统
  * 小象参考答案
  * Created by ch on 2016/8/25.
  */
object Recommend {

  def main(args: Array[String]) {
    // 在终端商过滤无关日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    if (args.length != 2) {
      println("Usage: /path/to/spark/bin/spark-submit --driver-memory 2g --class week7.MovieLensALS " + "week7.jar movieLensHomeDir personalRatingsFile")
//      sys.exit(1)
    }

    // 设置参数
    val conf = new SparkConf().setAppName("MovieLensALS").setMaster("local[2]")
    val sc = new SparkContext(conf)

   //修改train数据
    val data_train = sc.textFile("traintest.csv").map(_.replace("NULL","0")).filter(!_.contains("srch_id")).map(_.split(","))
    //求列数
//    data_train.map(x => println(x.size))
    //删除日期列:

    val data5date = data_train.map{x => val x2 = x.toBuffer;x2.remove(1,1);x2.toArray}

    data5date.foreach(println)

    //转换为libsvm格式:

    import scala.collection.mutable.ArrayBuffer

    val dataLibsvm= data5date.map {
      x =>
        val a = new ArrayBuffer[String]
        a += x.last
        for (i <- 0 to x.length - 1) {
          a += i + ":" + x(i)
        }
        a.toArray.mkString(" ")
    }

    //保存为一个文件
//    dataLibsvm.coalesce(1,true).saveAsTextFile("test2") /* */






    //修改测试数据
/*
    //读取数据,

    val data = sc.textFile("I:\\chinahadoop\\机器学习训练营\\训练营作业&代码\\推荐作业\\test.csv").map(_.replace("NULL","0")).filter(!_.contains("srch_id")).map(_.split(","))

    //求列数

    data.map(x => println(x.size))

    //删除日期列:

    val data5date = data.map{x => val x2 = x.toBuffer;x2.remove(1,1);x2.toArray}

    //转换为libsvm格式:

    import scala.collection.mutable.ArrayBuffer

    val dataLibsvm= data5date.map {
      x =>
        val a = new ArrayBuffer[String]
        a += "1"
        for (i <- 0 to x.length - 1) {
          a += i + ":" + x(i)
        }
        a.toArray.mkString(" ")
    }

    //保存为一个文件
    dataLibsvm.coalesce(1,true).saveAsTextFile("expedia_ml_test")*/

//       sc.textFile("xxxxxxxx").map(x=>x.replace("NULL","0")).map(x=>{val buf =x.split(",").toBuffer;buf.remove(1);buf}).map(x=>{for(i <-0 to 52){x(i)=i+":"+x(i)};x}).map(x=>x.mkString(","))

       // 训练模型
       import org.apache.spark.mllib.tree.GradientBoostedTrees
       import org.apache.spark.mllib.tree.configuration.BoostingStrategy
       import org.apache.spark.mllib.util.MLUtils

   val data =MLUtils.loadLibSVMFile(sc, "expedia_ml/part-00000")
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
}
