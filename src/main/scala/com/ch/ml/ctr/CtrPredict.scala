package com.ch.ml.ctr



import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * By: lxw
  * http://lxw1234.com
  */
object CtrPredict {

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
    val conf = new SparkConf().setMaster("yarn-client")
    val sc = new SparkContext(conf)

    val ctrRDD = sc.textFile("/tmp/lxw1234/sample.txt", 10)
    println("Total records : " + ctrRDD.count)

    //将整个数据集80%作为训练数据，20%作为测试数据集
    val train_test_rdd = ctrRDD.randomSplit(Array(0.8, 0.2), seed = 37L)
    val train_raw_rdd = train_test_rdd(0)
    val test_raw_rdd = train_test_rdd(1)

    println("Train records : " + train_raw_rdd.count)
    println("Test records : " + test_raw_rdd.count)

    //cache train, test
    train_raw_rdd.cache()
    test_raw_rdd.cache()

    val train_rdd = train_raw_rdd.map {
      line =>
        val tokens = line.split(",", -1)
        //key为id和是否点击广告
        val catkey = tokens(0) + "::" + tokens(1)
        //第6列到第15列为分类特征，需要One-Hot-Encoding
        val catfeatures = tokens.slice(5, 14)
        //第16列到24列为数值特征，直接使用
        val numericalfeatures = tokens.slice(15, tokens.size - 1)
        (catkey, catfeatures, numericalfeatures)
    }

    //拿一条出来看看
    train_rdd.take(1)
    //scala> train_rdd.take(1)
    //res6: Array[(String, Array[String], Array[String])] = Array((1000009418151094273::0,Array(1fbe01fe,
    //            f3845767, 28905ebd, ecad2386, 7801e8d9, 07d7df22, a99f214a, ddd2926e, 44956a24),
    //              Array(2, 15706, 320, 50, 1722, 0, 35, -1)))

    //将分类特征先做特征ID映射
    val train_cat_rdd = train_rdd.map {
      x => parseCatFeatures(x._2)
    }

    train_cat_rdd.take(1)
    //scala> train_cat_rdd.take(1)
    //res12: Array[List[(Int, String)]] = Array(List((0,1fbe01fe), (1,f3845767), (2,28905ebd),
    //        (3,ecad2386), (4,7801e8d9), (5,07d7df22), (6,a99f214a), (7,ddd2926e), (8,44956a24)))

    //将train_cat_rdd中的(特征ID：特征)去重，并进行编号
    val oheMap = train_cat_rdd.flatMap(x => x).distinct().zipWithIndex().collectAsMap()
    //oheMap: scala.collection.Map[(Int, String),Long] = Map((7,608511e9) -> 31527, (7,b2d8fbed) -> 42207,
    //  (7,1d3e2fdb) -> 52791
    println("Number of features")
    println(oheMap.size)

    //create OHE for train data
    val ohe_train_rdd = train_rdd.map { case (key, cateorical_features, numerical_features) =>
      val cat_features_indexed = parseCatFeatures(cateorical_features)
      val cat_feature_ohe = new ArrayBuffer[Double]
      for (k <- cat_features_indexed) {
        if (oheMap contains k) {
          cat_feature_ohe += (oheMap get (k)).get.toDouble
        } else {
          cat_feature_ohe += 0.0
        }
      }
      val numerical_features_dbl = numerical_features.map {
        x =>
          val x1 = if (x.toInt < 0) "0" else x
          x1.toDouble
      }
      val features = cat_feature_ohe.toArray ++ numerical_features_dbl
      LabeledPoint(key.split("::")(1).toInt, Vectors.dense(features))
    }

    ohe_train_rdd.take(1)
    //res15: Array[org.apache.spark.mllib.regression.LabeledPoint] =
    //  Array((0.0,[43127.0,50023.0,57445.0,13542.0,31092.0,14800.0,23414.0,54121.0,
    //     17554.0,2.0,15706.0,320.0,50.0,1722.0,0.0,35.0,0.0]))

    //训练模型
    //val boostingStrategy = BoostingStrategy.defaultParams("Regression")
    val boostingStrategy = BoostingStrategy.defaultParams("Classification")
    //    boostingStrategy.numIterations = 100
    //    boostingStrategy.treeStrategy.numClasses = 2
    //    boostingStrategy.treeStrategy.maxDepth = 10
    //    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()


    val model = GradientBoostedTrees.train(ohe_train_rdd, boostingStrategy)
    //保存模型
    model.save(sc, "/tmp/myGradientBoostingClassificationModel")
    //加载模型
    val sameModel = GradientBoostedTreesModel.load(sc, "/tmp/myGradientBoostingClassificationModel")

    //将测试数据集做OHE
    val test_rdd = test_raw_rdd.map { line =>
      val tokens = line.split(",")
      val catkey = tokens(0) + "::" + tokens(1)
      val catfeatures = tokens.slice(5, 14)
      val numericalfeatures = tokens.slice(15, tokens.size - 1)
      (catkey, catfeatures, numericalfeatures)
    }

    val ohe_test_rdd = test_rdd.map { case (key, cateorical_features, numerical_features) =>
      val cat_features_indexed = parseCatFeatures(cateorical_features)
      val cat_feature_ohe = new ArrayBuffer[Double]
      for (k <- cat_features_indexed) {
        if (oheMap contains k) {
          cat_feature_ohe += (oheMap get (k)).get.toDouble
        } else {
          cat_feature_ohe += 0.0
        }
      }
      val numerical_features_dbl = numerical_features.map { x =>
        val x1 = if (x.toInt < 0) "0" else x
        x1.toDouble
      }
      val features = cat_feature_ohe.toArray ++ numerical_features_dbl
      LabeledPoint(key.split("::")(1).toInt, Vectors.dense(features))
    }

    //验证测试数据集
    val b = ohe_test_rdd.map {
      y => val s = model.predict(y.features)
        (s, y.label, y.features)
    }

    b.take(10).foreach(println)

    //预测准确率
    val predictions = ohe_test_rdd.map(lp => sameModel.predict(lp.features))
    predictions.take(10).foreach(println)
    val predictionAndLabel = predictions.zip(ohe_test_rdd.map(_.label))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count / ohe_test_rdd.count
    println("GBTR accuracy " + accuracy)
    //GBTR accuracy 0.8227084119200302

  }

}

