package com.ch.ml.ctr

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  */
object CTRTrain {

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

    if (args.length != 3) {
      println("Usage:MakeTags <trainPath> <testPath> <modelPath>")
      System.exit(0)
    }
    val Array(trainPath, testPath, modelPath) = args
    //    val trainPath="part-train"
    //    val testPath = "part-test"
    //    , modelPath, trainOutput

    val conf = new SparkConf()
    //      .setMaster("local[2]").setAppName("xx")

    val sc = new SparkContext(conf)

    val splits = sc.textFile(trainPath).randomSplit(Array(0.7, 0.3), seed = 101L)
    val (trainRawData, testRawData) = (splits(0), splits(1))
    trainRawData.cache()
    testRawData.cache()

    val trainData = trainRawData.map {
      x =>
        val log = x.split(",", -1)
        val normal = log.slice(1, 24)
        val needofk = log.slice(24, log.size) // 后4列需要one of encoding
        (log(0), normal, needofk)
    }

    // one of encoding
    val oheMap = trainData.map(x => parseCatFeatures(x._3)).flatMap(x => x).distinct().zipWithIndex().collectAsMap()

    val trainLabelData = trainData.map {
      case (key, normal, ofk) =>
        val cat_features_indexed = parseCatFeatures(ofk)

        val featureNormal = normal.map { x =>
          try {
            (if (x isEmpty) "0" else x).toDouble
          } catch {
            case e:Exception => 0d
          }
        }

        val featureofk = new ArrayBuffer[Double]
        for (k <- cat_features_indexed) {
          if (oheMap contains k) {
            featureofk += (oheMap get (k)).get.toDouble
          } else {
            featureofk += 0.0
          }
        }
        val features = featureNormal ++ featureofk
        LabeledPoint(key.toInt, Vectors.dense(features))
    }


    // 把LablePoint数据转换为分层抽样支持的K-V数据
    val preData = trainLabelData.map(x => (x.label, x.features))
    // 对数据进行分层抽样  每一层都抽取0.8
    val fractions = preData.map(_._1).distinct.map(x => (x, 0.8)).collectAsMap

    // withReplacement = false 表示不重复抽样
    val sampleData = preData.sampleByKey(withReplacement = false, fractions, seed = 111L).map(x => LabeledPoint(x._1, x._2)).cache()
    //    val fractions: Map[Int, Double] = (List((1, 0.2), (2, 0.8))).toMap //表示在层1抽0.2，在层2中抽0.8
    // 训练 LR 模型
    /*val lrModel = new LogisticRegressionWithLBFGS()
      //        .setNumClasses(20)
      .run(sampleData)
    lrModel.save(sc, modelPath)
    // clear the default threshold
    lrModel.clearThreshold()
// 加载模型
//      lrModel.save(sc, "LR1-model" + outputPath)*/

    // 训练 GBTs
    val boostingStrategy = BoostingStrategy.defaultParams("Classification")
    boostingStrategy.numIterations = 100
    boostingStrategy.treeStrategy.numClasses = 2
    boostingStrategy.treeStrategy.maxDepth = 10
    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()


    val model = GradientBoostedTrees.train(sampleData, boostingStrategy)


    val testLabelData = testRawData.map {
      x =>
        val log = x.split(",", -1)
        val normal = log.slice(1, 24)
        val needofk = log.slice(24, log.size)
        //        (log(0), normal, needofk)
        //    }.map {
        //      case (key, normal, ofk) =>
        val cat_features_indexed = parseCatFeatures(needofk)

        val featureNormal = normal.map { x =>
          try {
            (if (x isEmpty) "0" else x).toDouble
          } catch {
            case e:Exception => 0d
          }
        }

        val featureofk = new ArrayBuffer[Double]
        for (k <- cat_features_indexed) {
          if (oheMap contains k) {
            featureofk += (oheMap get (k)).get.toDouble
          } else {
            featureofk += 0.0
          }
        }
        val features = featureNormal ++ featureofk
        LabeledPoint(log(0).toInt, Vectors.dense(features))
    }


    val lableAndPredict = testLabelData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
//    val predictAndLable = predictions.zip(testLabelData.map(_.label)) // (pre lable,true lable)

    val testErr = lableAndPredict.filter(x => x._1 != x._2).count().toDouble / testLabelData.count()

    println("LR accuracy " + testErr)
  }
}
