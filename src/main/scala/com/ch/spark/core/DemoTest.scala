package main.scala.com.ch.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ch on 2017/2/17.
  * 面试题，给定两个数据集
  * 数据集A id,age,name
  * 数据集B    id,year,month,movie
  * 要求，输出：id,age,name,year,month,movie(同一个用户，按year升序，没有数据B的id，用null补
  */
object DemoTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[1]").setAppName("xx")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)



    val rdda = sc.textFile("src/main/scala/com/ch/spark/core/a.txt").map{
      x=>
        val adata=x.split(" ",2)
        (adata(0),adata(1))
    }
    val rddb = sc.textFile("src/main/scala/com/ch/spark/core/b.txt",2)/*.foreach(println)*/
      .map{
      x=>
        val adata=x.split(" ",2)
        (adata(0),adata(1))
    }

    rdda.union(rddb).groupByKey().coalesce(1)
      .map{
        case(k,v)=>
          val arrs = v.slice(1,v.size).toList
          val key = k.concat(" ").concat(v.head)
          //        println("head=",v.head,key)
          //        var values:List[(String,String)]=List(("null","null null"))
          var values:String=""
          if(arrs.length==0){
            values = "null null null"
          }else{
            values =
              arrs./*toList.*/map {
                v2 =>
                  val arrs2 = v2.split(" ", 2)
                  //            println(arrs(0), arrs(1))
                  (arrs2(0), arrs2(1))
              }
                .sortWith(_._1.toInt < _._1.toInt)
                //             .reduce(_._1 ++ _._2).map(x=>x) // 类型成char了
                .map(x=>x._1+" " ++ x._2)
                .reduce(_+" " ++ _)
            //            .foreach(print)

            //          .flatMap(x=>x._1.concat(x._2))//
            //           .mkString(" ")
            //          println("-----"+values)
          }
          key.concat(",").concat(values)
      }
      .collect()
      .foreach(println)

    // (u5,CompactBuffer(22 xa2))(u1,CompactBuffer(12 zs, 2016 9 m1))(u2,CompactBuffer(15 xx, 2017 12 m2))(u3,CompactBuffer(18 aaa, 2017 1 m3, 2014 2 m4, 2012 3 m5))(u4,CompactBuffer(20 xa1))

  }
}
