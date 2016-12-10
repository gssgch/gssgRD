package com.ch.spark.xxspark

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

// Import Spark SQL data types
import org.apache.spark.sql.types._
;

/**
  * xiao xiang spark
  * 统计pv 和 uv
  * 使用spark sql 重构
  */
object SogouSparkSql {

  def main(args: Array[String]) {
    if (args.length != 3) {
      println("Usage:SogouSparkSql <inputLogBase> <parquetName> <outputPath> ")
      System.exit(0)
    }
    val Array(inputLogBase, parquetName, outputPath) = args
    val conf = new SparkConf()
    .setAppName("SogouSparkSql") //.setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val people = sc.textFile(inputLogBase)
    val schemaString = "hour,uid"
    val schema =
      StructType(
        schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    // Convert records of the RDD (people) to Rows.
    val rowRDD = people.map(_.split("\t")).map(p => Row((p(0).split(":")(0).trim), p(1).trim))
    val userDF = sqlContext.createDataFrame(rowRDD, schema)
    userDF /*.repartition("1")*/ .saveAsParquetFile(parquetName)

    val parquetFile = sqlContext.parquetFile(parquetName).repartition(1)
    parquetFile.registerTempTable("soGou")
    val teenagers = sqlContext.sql("SELECT hour,count(uid),count(distinct uid) FROM soGou group by hour order by hour")
    teenagers.map(t => "[" + t(0) + "],[" + t(1) + "],[" + t(2) + "]").repartition(1) saveAsTextFile (outputPath)
  }
}


