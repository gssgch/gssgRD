package com.ch.ml.expedia

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{LogisticRegressionWithSGD, LogisticRegressionWithLBFGS}
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

    if (args.length != 3) {
      println("Usage:ExpediaTrain <trainPath> <testPath> <outputPath>")
      //      System.exit(0)
    }
    val args1 = Array("file:///home/hive/bj/ch/expedia/train/part-*"
      , "file:///home/hive/bj/ch/expedia/test/part-*"
      , "file:///home/hive/bj/ch/expedia/"
    )
    val Array(trainPath, testPath, outputPath, flag) = args

    // 设置参数
    val conf = new SparkConf() /*.setAppName("expediaTrain").setMaster("local[2]")*/
    val sc = new SparkContext(conf)


    val trainData = MLUtils.loadLibSVMFile(sc, trainPath)
    val testData = MLUtils.loadLibSVMFile(sc, testPath)
    // train数据3 7 分
    //    val splits = data.randomSplit(Array(0.7, 0.3))
    //    val (trainingData, testData) = (splits(0), splits(1))

    // 把LablePoint数据转换为分层抽样支持的K-V数据
    val preData = trainData.map(x => (x.label, x.features))
    // 对数据进行分层抽样  每一层都抽取0.8
    val fractions = preData.map(_._1).distinct.map(x => (x, 0.8)).collectAsMap
    // withReplacement = false 表示不重复抽样
    val sampleData = preData.sampleByKey(withReplacement = false, fractions, seed = 101L).map(x => LabeledPoint(x._1, x._2))/*.cache()*/
    //    val fractions: Map[Int, Double] = (List((1, 0.2), (2, 0.8))).toMap //表示在层1抽0.2，在层2中抽0.8

    if (flag.equals("1")) {
      // lr1
      // 训练 LR 模型
      val lrModel = new LogisticRegressionWithLBFGS().run(sampleData)
      // clear the default threshold
      lrModel.clearThreshold()

      //计算泛化误差
      val testErr = testData.map {
        point =>
          val score = lrModel.predict(point.features)
          score.toFloat + "," + point.label.toInt + "," + point.features(1).toInt + "," + point.features(7).toInt
      }.coalesce(1, true).saveAsTextFile("LR1" + outputPath)

      lrModel.save(sc, "LR1-model" + outputPath)
    } else if (flag.equals("2")) {
      //lr2
      val lrModel2 = new LogisticRegressionWithSGD().run(sampleData)
      lrModel2.clearThreshold()

      // LR model 2
      testData.map {
        point =>
          val score = lrModel2.predict(point.features)
          score.toFloat + "," + point.label.toInt + "," + point.features(1).toInt + "," + point.features(7).toInt
      }.coalesce(1, true).saveAsTextFile("LR2" + outputPath)

      lrModel2.save(sc, "LR2-model" + outputPath)

    } else if (flag.equals("3")) {
      // gbdt
      // 训练GBDT模型
      val boostingStrategy = BoostingStrategy.defaultParams("Classification") /*.setNumIterations(3)*/
      val GBDTModel = GradientBoostedTrees.train(sampleData, boostingStrategy)

      // GBDTModel
      testData.map {
        point =>
          val score = GBDTModel.predict(point.features)
          score.toFloat + "," + point.label.toInt + "," + point.features(1).toInt + "," + point.features(7).toInt
      }.coalesce(1, true).saveAsTextFile("GBDT" + outputPath)

      GBDTModel.save(sc, "GBDT-model" + outputPath)
    }





    // 计算测试误差
    /*   val testErr = testData.map { point => val prediction = model.predict(point.features)
         if (point.label == prediction) 1.0 else 0.0
       }.mean()
       println("Test Error = " + testErr)
       println("Learned GBT model:n" + model.toDebugString)*/

    // 0.9720347763749159


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
