package com.ch.ml.ctr

import org.apache.spark.ml.feature._
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 练习，from spark.apache.org
  */
object Convert {

  def main(args: Array[String]) {


    if (args.length != 4) {
      println("Usage:MakeTags <inputLogBase> <outputPath1> <outputPath1> <partition>")
//      System.exit(0)
    }
//    val Array(inputLogBase, outputPath,outputPath2, pars) = args

//        val masterId = "local[2]"
//        val inputLogBase = "F:\\company\\lomark\\平台数据&自动化脚本\\原始数据parquet文件\\2016-10-01_00_p1_invalid.1475254927494.log.20161001012611215.parquet"
//        val outputPath = "F:\\output\\"
//        val pars = "3"


    val conf = new SparkConf()
          .setMaster("local[2]").setAppName("xx")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[CTRBean]))

    val sc = new SparkContext(conf)
    // 实际使用的
    val sqlc = new SQLContext(sc)
    sqlc.setConf("spark.sql.parquet.compression.codec", "snappy")


    /*var srcDF = sc.textFile("/tmp/lxw1234/1.txt").map {
      x =>
        var data = x.split(",")
        RawDataRecord(data(0), data(1))
    }.toDF("", "")
    var tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    var wordsData = tokenizer.transform(srcDF)

    var hashingTF =
      new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(100)
    var featurizedData = hashingTF.transform(wordsData)
    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    var idfModel = idf.fit(featurizedData)
    var rescaledData = idfModel.transform(featurizedData)
    var trainDataRdd = rescaledData.select($"category",$"features").map {
      case Row(label: String, features: Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }*/


    val sentenceData = sqlc.createDataFrame(Seq(
//      (0, "Hi I heard about Spark"),
//      (0, "I wish Java could use case classes"),
//      (1, "Logistic regression models are neat")
        (0, "张三 李四 王五"),
        (0, "张四2 李三2 王六2"),
        (1, "张说三3 李是四3 王去五3")

    )).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)
    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(120)
    val featurizedData = hashingTF.transform(wordsData)
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)
//    rescaledData.select("label", "features").take(3).foreach(println)
    rescaledData.select("label", "words", "features").take(3).foreach(println)
    rescaledData.select("label", "features").map {
      case Row(label: String, features: Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }.foreach(println)


    /*val df = sqlc.createDataFrame(Seq(
      (0, "a"),
      (1, "b"),
      (2, "c"),
      (3, "a"),
      (4, "a"),
      (5, "c")
    )).toDF("id", "category")

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")
      .fit(df)
    val indexed = indexer.transform(df)

    val encoder = new OneHotEncoder()
      .setInputCol("categoryIndex")
      .setOutputCol("categoryVec")
    val encoded = encoder.transform(indexed)
    encoded.select("id", "categoryVec").show()*/


  }
}
case class RawDataRecord(label: String, features: String)