package main.scala.com.ch.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Test {
  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[2]").setAppName("test")

    val sc = new SparkContext(conf)

    val s = "dP4KnqwQAP"
    println(theLongest(s))

    /* sc.textFile("").coalesce(10).first()
       .repartition(10)
       .flatMap(x=>x+1)

     val rdd =sc.parallelize(List(List(1,2,3),List(2,3,4))).cache()
       println("1="+rdd.first())
         println("2="+rdd.take(1))*/

    val kv1 = sc.parallelize(List(("A", 1), ("B", 2), ("C", 3), ("A", 2), ("B", 1), ("A", 3)))
    kv1.groupBy { case (k, v) => k }.map { case (k, v) => {
      println(k + "," + v)
    }
    }.collect

    /*sc.textFile("aaa.txt").map(x => x.split(",", x.length)).filter(_.size>3).flatMap {
      log =>
        log(1).trim.split("\\|").map {
          key =>
            (log(0), key, log(2), log(3))
        }
    }.groupBy { case (ad, key, _, _) => ad + "_" + key }.map {
      case (k, v) =>
        val imp = v.map { case (_, _, imp, _) => imp.toInt }.sum
        val click = v.map { case (_, _, _, click) => click.toInt }.sum
        println("===log="+k.split("_")(0)+","+k.split("_")(1)+","+imp+","+click)
        (k.split("_")(0), k.split("_")(1), imp, click)
    }

   sc.parallelize(List(("A",1),("B",2),("C",3),("A",4),("B",5)))
    .sortByKey()
        .groupByKey()
        .saveAsTextFile("")
        .cogroup(rdd.flatMap().map((_,1)))
      .sortByKey()
     rdd .map({
        x=>
          x.map(_+1)
      }).foreach(println)
    rdd.flatMap({
      x=>x.map(_+1)
    }).foreach(println)*/
  }

  /**找到包含大写和小写字符，但不包含数组的最长字符串*/
  def theLongest(s: String): String = {
    s.split("[0-9]")
      //      .filter(_.exists((ch=>ch.isUpper)))
      //      .filter(_.exists(_.isLower))
      .filter(x => x.exists(_.isUpper) && x.exists(_.isLower))
      .maxBy(_.length)
  }
}
