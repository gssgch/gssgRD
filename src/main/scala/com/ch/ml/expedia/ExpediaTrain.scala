package com.ch.ml.expedia

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{LogisticRegressionWithSGD, LogisticRegressionWithLBFGS, LogisticRegressionModel}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by ch on 2016/8/25.
  */
object ExpediaTrain {

  def main(args: Array[String]) {
    // 在终端商过滤无关日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    if (args.length != 2) {
      println("Usage:ExpediaTrain <filePath> <fileTpye> <outputPath>")
      System.exit(0)
    }
    val Array(filePath, fileType, outputPath) = args

    // 设置参数
    val conf = new SparkConf().setAppName("expediaTrain").setMaster("local[2]")
    val sc = new SparkContext(conf)


    val trainData = MLUtils.loadLibSVMFile(sc, "expedia_ml/part-00000")
    val testData = MLUtils.loadLibSVMFile(sc, "expedia_ml_test/part-00000")
    // train数据3 7 分
    //    val splits = data.randomSplit(Array(0.7, 0.3))
    //    val (trainingData, testData) = (splits(0), splits(1))

    // 把LablePoint数据转换为分层抽样支持的K-V数据
    val preData = trainData.map(x => (x.label, x.features))
    // 对数据进行分层抽样  每一层都抽取0.8
    val fractions = preData.map(_._1).distinct.map(x => (x, 0.8)).collectAsMap
    // withReplacement = false 表示不重复抽样
    val sampleData = preData.sampleByKey(withReplacement = false, fractions, seed = 101L)
      .map(x => LabeledPoint(x._1, x._2)).cache()

    //    val fractions: Map[Int, Double] = (List((1, 0.2), (2, 0.8))).toMap //表示在层1抽0.2，在层2中抽0.8

    // 训练 LR 模型
    val lrModel = new LogisticRegressionWithLBFGS().run(sampleData)

    val lrModel2 = new LogisticRegressionWithSGD().run(sampleData)


    // clear the default threshold
    lrModel.clearThreshold()

    // 训练GBDT模型
    val boostingStrategy = BoostingStrategy.defaultParams("Classification") /*.setNumIterations(3)*/
    val bsModel = GradientBoostedTrees.train(sampleData, boostingStrategy)

    // 计算测试误差
    /*   val testErr = testData.map { point => val prediction = model.predict(point.features)
         if (point.label == prediction) 1.0 else 0.0
       }.mean()
       println("Test Error = " + testErr)
       println("Learned GBT model:n" + model.toDebugString)*/

    // 0.9720347763749159

    //计算泛化误差
    val testErr = testData.map {
      point =>
        val score = lrModel.predict(point.features)
        score.toFloat+","+ point.label.toInt+","+ point.features(1).toInt+","+ point.features(7).toInt
    }.coalesce(1, true).saveAsTextFile(outputPath)

    lrModel.save(sc,outputPath+"_model")

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
