package com.ch.spark.ml.movieRecommend

import org.apache.spark.{SparkConf, SparkContext}


/**

  */

object RecommendTest {
  val K=10
  def main(args: Array[String]) {
    val conf = new SparkConf()
        .setMaster("local")
        .setAppName("MovieLensRecommend")
    val sc = new SparkContext(conf)

    val file="K:\\MLearning\\training\\code\\sparkMLMovieRecommend\\ml-1m/ratings.dat"
    val file2="K:\\MLearning\\training\\code\\sparkMLMovieRecommend\\ml-1m/movies.dat"
    /**提取特征(影片 ID 星级 事件戳)*/
    val rawData = sc.textFile(file)

    val rawRatings = rawData.map(_.split("::").take(3))
    import org.apache.spark.mllib.recommendation.{ALS, Rating}
    val ratings = rawRatings.map { case Array(user, movie, rating) => Rating(user.toInt, movie.toInt, rating.toDouble)}
    ratings.first()

    /**训练推荐模型*/
    val rank = 50 //因子个数
    val iteratings = 10  //迭代
    val lambda = 0.01   //正则化参数
    // 使用ALS建立模型
    val model = ALS.train(ratings, rank, iteratings, lambda)
    /**使用推荐模型*/

    val userID = 789
    val K = 10
    val topKRecs = model.recommendProducts(userID,K)
    println(topKRecs.mkString("\n"))

    /**检验推荐内容*/
    val movies = sc.textFile(file2)
    val titles = movies.map(line => line.split("::").take(2)).map(array => (array(0).toInt, array(1))).collectAsMap()
    titles(123)

    val user = 798
    //keyBy创建键值对RDD，lookup只返回给定键值
    val moviesForUser = ratings.keyBy(_.user).lookup(user)
    println("该用户评价过多少电影:"+moviesForUser.size) //该用户评价过多少电影
    //查看789用户评价最高的10部电影
//    moviesForUser.sortBy(-_.rating).take(10).map(rating => (titles(rating.product),rating.rating)).foreach(println)
//    moviesForUser.sortBy(-_.rating).take(10).map(rating => (user,rating.product,titles(rating.product),rating.rating)).foreach(println)
    // 查看798用户的所有电影评分
    printf("\n查看用户 "+user+" 的所有电影评分：\n")
    moviesForUser.sortBy(-_.rating).map(rating => (user,rating.product,rating.rating)).foreach(println)
    //查看给789用户推荐的前10部电影
    printf("\n给用户 "+user+" 推荐的前10部电影如下：\n")
    topKRecs.map(rating => (titles(rating.product), rating.rating)).foreach(println)
 }
}