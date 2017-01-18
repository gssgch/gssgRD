package main.scala.com.ch.spark.ml.movieRecommend

import org.apache.spark.{SparkConf, SparkContext}


/**
  * 电影数据分析

  */

object MovieDataAnalysis {
  val K=10
  def main(args: Array[String]) {
    val conf = new SparkConf()
        .setMaster("local[1]")
        .setAppName("MovieLensRecommend")
    val sc = new SparkContext(conf)

    val file="K:\\MLearning\\training\\code\\sparkMLMovieRecommend\\ml-1m/ratings.dat"
    val userFile="K:\\MLearning\\training\\code\\sparkMLMovieRecommend\\ml-1m/users.dat"
    val file2="K:\\MLearning\\training\\code\\sparkMLMovieRecommend\\ml-1m/movies.dat"

    // 1::F::1::10::48067   用户id,性别，年龄，职业，邮编
    val userData = sc.textFile(userFile).map(_.split("::"))
//    sc.textFile(userFile).take(10).foreach(println)

    // 统计年龄 并按升序排列
    userData.map(_(2)).map((_,1)).reduceByKey(_ + _).sortBy(_._1).foreach(print)

    // 统计职业
//    userData.map(_(3)).countByValue().foreach(print)

    // 1::1193::5::978300760  UserID::MovieID::Rating::Timestamp
//    sc.textFile(file).first().foreach(println)
    val ratData = sc.textFile(file).map(_.split("::"))
    // 评分统计分析
    val stats=ratData.map(_(2).toInt).stats()
//    print(stats)
//    (count: 1000209, mean: 3.581564, stdev: 1.117101, max: 5.000000, min: 1.000000)

    val ratany=ratData.map(_(0)).countByValue().foreach(println)

    // 1::Toy Story (1995)::Animation|Children's|Comedy
    val movieData = sc.textFile(file2).map(_.split("::"))
//    sc.textFile(file2).first().foreach(println)




    sys.exit(0)




 }
}