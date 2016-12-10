package com.ch.spark.ml.expedia

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS, LogisticRegressionWithSGD}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ch on 2016/8/25.
  */
object ExpediaTrainTest {

  def main(args: Array[String]) {
    // 在终端商过滤无关日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    if (args.length != 3) {
      println("Usage:ExpediaTrain <trainPath> <testPath> <outputPath>")
      //      System.exit(0)
    }
    val args1 = Array("file:///home/hive/bj/ch/expedia/expedia_ml/part-00000"
      , "file:///home/hive/bj/ch/expedia/expedia_ml_test/part-00000"
      , "file:///home/hive/bj/ch/expedia/expedia_out"
      , "file:///home/hive/bj/ch/expedia/expedia_out_model"
      , "1"
    )
val args2 = Array("i:\\chinahadoop\\机器学习训练营\\训练营作业&代码\\3，推荐系统项目\\xxdata\\expedia_ml\\part-00000"
  , "i:\\chinahadoop\\机器学习训练营\\训练营作业&代码\\3，推荐系统项目\\xxdata\\expedia_ml_test\\part-00000"
  , "expedia_train_out"
  ,""
  ,"1"
)

    val args3 = Array("/user/s-56/expedia_ml/part-00000"
      , "/user/s-56/expedia_ml_test/part-00000"
      , "/user/s-13/expedia_train_out"
      , "/user/s-13/expedia_train_out_model"
      ,"1"
    )
    val Array(trainPath, testPath, outputPath,outputPathModel, flag) = args2

    // 设置参数
    val conf = new SparkConf() .setAppName("expediaTrain").setMaster("local[2]")
    val sc = new SparkContext(conf)


//    val trainData = MLUtils.loadLibSVMFile(sc,trainPath)
    val testData = MLUtils.loadLibSVMFile(sc, testPath)
      .saveAsTextFile("szs-"+outputPath)



  }


}
