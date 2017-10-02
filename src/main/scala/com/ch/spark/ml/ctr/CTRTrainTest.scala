package com.lomark.ctr

import com.ch.spark.ml.ctr.CTRBean
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS, LogisticRegressionWithSGD}
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/** local test 20170118
  */
object CTRTrainTest {

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
      //      System.exit(0)
    }

    val masterId = "local[2]"
    val inputLogBase = "F:\\company\\lomark\\平台数据&自动化脚本\\原始数据parquet文件\\2016-10-01_00_p1_invalid.1475254927494.log.20161001012611215.parquet"
    //            val outputPath = "F:\\output\\"
    val pars = "3"


    val conf = new SparkConf()
      .setMaster("local[2]").setAppName("xx")
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

      .map {
        // 改造实验数据
        x =>
          x.sessionid = (math.random * 10 % 2).floor.toInt.toString
          x.requestmode = (if (math.random * 10 % 2 > 1) 2 else 1)
          x.toString
      }
      // 0,0,0,0,100018,0.0,0.0,2,4,3,9,1,4,未知,未知,0,游戏世界|经典游戏|单机游戏|内容

      /* .foreach{
         x=>
           if( x.requestmode == 2){
           println("adorderid="+x.adorderid+",requestmode="+x.requestmode+"")
           }
       }*/
      //  /*.take(20)*/.foreach(println)
      .randomSplit(Array(0.7, 0.3), seed = 101L)
    val (trainRawData, testRawData) = (data(0), data(1))
    val trainData = trainRawData.map {
      x =>
        val log = x.split(",", -1)
        // 共17
        val normal = log.slice(1, 13)
        val needofk = log.slice(13, log.size) // 后4列需要one of encoding
        (log(0), normal, needofk)
    }


    // one of encoding  把每一个特征都转换为一个数字  相当于弄了一个字典 或者一个元组 key是(位置，特征)  value是转换后的数字
    val oheMap = trainData.map(x => parseCatFeatures(x._3)).flatMap(x => x).distinct().zipWithIndex().collectAsMap()
    /* ((2,会说话的汤姆猫好吃展示（上）),64505)
     ((2,《泊车殿下》激情戏_超清),49454)*/

    val trainLabelData = trainData.map {
      case (key, normal, ofk) =>
        val cat_features_indexed = parseCatFeatures(ofk)
        // List((0,未知), (1,未知), (2,0), (3,熊出没|生化危机|熊出没|生化危机))  特征和编号

        val featureNormal = normal.map { x =>
          try {
            (if (x isEmpty) "0" else x).toDouble
          } catch {
            case e: Exception => 0d
          }
        }
        val featureofk = new ArrayBuffer[Double]
        for (k <- cat_features_indexed) {
          /* k=(3,喜剧|魔幻片|高清影院|地区)
           k=(0,未知)
           k=(1,未知)
           k=(2,0)*/
          if (oheMap contains k) {
            featureofk += (oheMap get (k)).get.toDouble
            //println("v="+featureofk) // ArrayBuffer(87254.0, 48422.0, 42224.0, 14493.0)
          } else {
            featureofk += 0.0
          }
        }
        val features = featureNormal ++ featureofk
        //         println("fe="+features.toList)
        // List(0.0, 0.0, 0.0, 100004.0, 0.0, 0.0, 1.0, 1.0, 3.0, 12.0, 1.0, 4.0, 87254.0, 48422.0, 11719.0, 10052.0)
        LabeledPoint(key.toInt, Vectors.dense(features))
    }

    val lrModel = LogisticRegressionWithSGD.train(trainLabelData,2)

    // 可提取公共方法
    val testLabelData = testRawData.map {
      x =>
        val log = x.split(",", -1)
        // 共17
        val normal = log.slice(1, 13)
        val needofk = log.slice(13, log.size) // 后4列需要one of encoding
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



    val lableAndPredict = testLabelData.map {
      case LabeledPoint(label,features)=>
        val prediction = lrModel.predict(features)
//        println("pre=="+prediction)
        (prediction,label)
    }

    // Get evaluation metrics.
    val metrics = new MulticlassMetrics(lableAndPredict)
    val accuracy = metrics.precision
    println(s"------------Accuracy = $accuracy")

    Seq(lrModel).map {
      // lr模型和svm模型可以这样计算
      model =>
        val scoreAndLabels = testLabelData.map {
          point => (model.predict(point.features), point.label)

        }
        val metrics = new BinaryClassificationMetrics(scoreAndLabels)
        (model.getClass.getSimpleName, metrics.areaUnderPR(), metrics.areaUnderROC())
            // 2轮时
        // ------------Accuracy = 0.5011143634013343
        // (LogisticRegressionModel,0.7486349048333656,0.5)
    }.foreach(println)




    //      .filter { log => log.adorderid > 0 && (log.requestmode == 1 || log.requestmode == 2) }

    /*
    .groupBy { case k => k.sessionid }.map { // 以展示和请求数据为 点击和展示数据，不然数据质量差，没有数据
    x =>
      val reqBean = x._2.find(_.requestmode == 1)
      val clkBean = x._2.find { log => log.requestmode == 2 /*&& log.iseffective == 1*/ }
      if (!clkBean.isEmpty && !reqBean.isEmpty) {
        reqBean.get.requestmode = clkBean.get.requestmode // click
        reqBean.get.sessionid = "1" // label  click
        reqBean.get.toString
      } else if (!reqBean.isEmpty /*&& math.random>0.7*/) {
        reqBean.get.sessionid = "0"
        //          println("xxxx===",reqBean.get.sdkversionnumber,reqBean.get.adplatformkey,reqBean.get.title,reqBean.get.keywords)
        reqBean.get.toString
      } else {
        null
      }
  }.foreach(println)*/
    //      .filter(_ != null)

    /*
        val Array(trainPath, testPath, modelPath) = args
        //    val trainPath="part-train"
        //    val testPath = "part-test"
        //    , modelPath, trainOutput

        val conf = new SparkConf()
        //      .setMaster("local[2]").setAppName("xx")

        val sc = new SparkContext(conf)

        val splits = sc.textFile(trainPath)
    //      .repartition(1)
            .randomSplit(Array(0.7, 0.3), seed = 101L)
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

        // 训练 LR 模型
        val lrModel = new LogisticRegressionWithLBFGS()
                  .setNumClasses(15)
          .run(trainLabelData)
    //    lrModel.save(sc, modelPath)
        // clear the default threshold
        lrModel.clearThreshold()
    // 加载模型
    //      lrModel.save(sc, "LR1-model" + outputPath)

        // 训练 GBTs
        /*val boostingStrategy = BoostingStrategy.defaultParams("Classification")
        boostingStrategy.numIterations = 100
        boostingStrategy.treeStrategy.numClasses = 2
        boostingStrategy.treeStrategy.maxDepth = 10
        boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()


        val model = GradientBoostedTrees.train(sampleData, boostingStrategy)*/


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


        val lableAndPredict = testLabelData.map {
          case LabeledPoint(label,features)=>
          val prediction = lrModel.predict(features)
          (prediction,label)
        }

        // Get evaluation metrics.
        val metrics = new MulticlassMetrics(lableAndPredict)
        val accuracy = metrics.precision
        println(s"------------Accuracy = $accuracy")*/

    /*
          // GBT 预测效果

        val testErr = lableAndPredict.filter(x => x._1 != x._2).count().toDouble / testLabelData.count()
        println("LR accuracy " + testErr)*/
  }
}
