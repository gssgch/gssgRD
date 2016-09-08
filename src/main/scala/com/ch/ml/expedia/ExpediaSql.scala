package com.ch.ml.expedia

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ch on 2016/8/31.
  */
object ExpediaSql {
  //使用spark SQL 进行排序
  import org.apache.spark.sql.Row
  import org.apache.spark.sql.types._

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("SoGou").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val reRDD = sc.textFile("part-00000.log")
    val schemaString = "prob label search hotel"
    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    val rowRDD = reRDD.map(_.replace("(","").replace(")","").split(",")).map(attributes => Row(attributes(0), attributes(1),attributes(2),attributes(3)))
    val sqlContext = new SQLContext(sc)



    val rmDF = sqlContext.createDataFrame(rowRDD, schema).repartition(1)
    rmDF.registerTempTable("recommendation")
    rmDF.groupBy()
//    rmDF.groupBy("search")

    val results = sqlContext.sql("SELECT prob,hotel,search,label FROM recommendation  Order By search, prob desc")
//    results.show()
//    results.repartition(1).save("result.log","json")
    for (row<- results) {
     println(row.get(0) + "," + row.get(1) + "," + row.get(2)+","+row.get(3))
    }
//    results.foreach{
//      x=>
//        println(x(0).asInstanceOf[String]+","+x(1))
//    }
//    val results = sqlContext.sql("SELECT search,first_value(prob),first_value(label),first_value(hotel) FROM recommendation Group By search Order By prob desc")
//    val results = sqlContext.sql("SELECT * FROM recommendation ")



//  case class Person(prob:Long, label:Int, search:Int,hotel:Int)
//  val people = sc.textFile("part-00000.log").map(_.replace("(","").replace(")","").split(",")).map(p => Person(p(0).trim.toLong, p(1).trim.toInt,p(2).trim.toInt,p(3).trim.toInt)).toDF()
//    people.registerTempTable("people")
//    val teenagers = sqlContext.sql("SELECT prob,hotel,search,label FROM people group by search  Order By prob desc")
//
//
//    teenagers.map(t => "" + t(0)+""+t(1)+","+t(2)+","+t(3)).collect().foreach(println)
  }
}
