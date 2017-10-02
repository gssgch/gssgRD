package main.scala.com.ch.spark.ml.movieRecommend

import org.apache.spark.{SparkConf, SparkContext}


/**
  * 电影推荐之用户推荐  ALS
  *
  */

object RecommendTest2 {
  val K = 10

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("MovieLensRecommend")
    val sc = new SparkContext(conf)

    val file = "src/main/scala/com/ch/spark/ml/movieRecommend/data/ratings.txt"
    val file2 = "K:\\MLearning\\training\\code\\sparkMLMovieRecommend\\ml-1m/movies.dat"
    /** 提取特征(影片 ID 星级 事件戳) */
    val rawData = sc.textFile(file)

    val rawRatings = rawData.map(_.split("::").take(3))
    import org.apache.spark.mllib.recommendation.{ALS, Rating}
    val ratings = rawRatings.map { case Array(user, movie, rating) => Rating(user.toInt, movie.toInt, rating.toDouble) }
//    ratings.first()

    /** 训练推荐模型 */
    val rank = 30 //因子个数
    val iteratings = 12 //迭代
    val lambda = 0.01 //正则化参数
    // 使用ALS建立模型
    ALS.train(ratings, rank, iteratings, lambda)
      .recommendProductsForUsers(2)
        .map(x=>(x._1,x._2(0).product,x._2(0).rating,
          x._2(1).product,x._2(1).rating))
      .foreach(println)

    sys.exit(0)

    val model = ALS.train(ratings, rank, iteratings, lambda)

    /** predict函数方便的计算 给定用户对给定物品的预期评分 */
    val predictScore = model.predict(789, 123)
    println("predictScore=" + predictScore)

    /** 使用推荐模型 为指定用户生成前k个推荐 */
    val userID = 789
    val K = 10
    val topKRecs = model.recommendProducts(userID, K)
    println(topKRecs.mkString("\n"))


    /** 检验推荐内容 */
    val movies = sc.textFile(file2)
    val titles = movies.map(line => line.split("::").take(2)).map(array => (array(0).toInt, array(1))).collectAsMap()
    titles(123)

    val user = 798
    //keyBy创建键值对RDD，lookup只返回给定键值
    val moviesForUser = ratings.keyBy(_.user).lookup(user)

    /*
      println("该用户评价过多少电影:"+moviesForUser.size) //该用户评价过多少电影
      //查看789用户评价最高的10部电影
  //    moviesForUser.sortBy(-_.rating).take(10).map(rating => (titles(rating.product),rating.rating)).foreach(println)
  //    moviesForUser.sortBy(-_.rating).take(10).map(rating => (user,rating.product,titles(rating.product),rating.rating)).foreach(println)
      // 查看798用户的所有电影评分
      printf("\n查看用户 "+user+" 的所有电影评分：\n")
      moviesForUser.sortBy(-_.rating).map(rating => (user,rating.product,rating.rating)).foreach(println)
   */
      //查看给789用户推荐的前10部电影
      printf("\n给用户 "+user+" 推荐的前10部电影如下：\n")
      topKRecs.map(rating => (titles(rating.product), rating.rating)).foreach(println)


    sys.exit(0)

    /** 下面是物品推荐
      * 给定一个物品，有哪些物品与它最相似？
      *
      * 利用余弦相似度来对指定物品的因子向量与其他物品的做比较。
      * */

    import org.jblas.DoubleMatrix
    val aMatrix = new DoubleMatrix(Array(1.0, 2.0, 3.0))


    // 余玄相似度：两个向量的点积/各向量范数的乘积  L2正则化了的点积 1表示完全相似 0表两者互不相关
    def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
      vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
    }
    val itemId = 567
    val itemFactor = model.productFeatures.lookup(itemId).head
    val itemVector = new DoubleMatrix(itemFactor)
    // 计算物品相似度
    val similar = cosineSimilarity(itemVector, itemVector)
    println("相似度=" + similar)


    //求各个物品与567的余玄相似度
    val sims = model.productFeatures.map { case (id, factor) =>
      val factorVector = new DoubleMatrix(factor)
      val sim = cosineSimilarity(factorVector, itemVector)
      (id, sim)
    }
    /** 取出与物品567最相似的前10个物品 */
    val sortedSims = sims.top(K)(Ordering.by[(Int, Double), Double] { case (id, similarity) => similarity })
    println(sortedSims.mkString("\n"))

    /*  相似物品结果
      (567,0.9999999999999998)
      (719,0.7483422118140968)
      (940,0.7278745617680672)
      (109,0.7252762702844262)
      (1376,0.7140740036831982)
      (201,0.7108942599667418)
      (184,0.7071733229661871)
      (1007,0.7027410411824709)
      (670,0.7001937255541564)
      (288,0.6987844388998511)
 */


    /** 检查推荐的相似物品 */
    println(titles(itemId))
    val sortedSims2 = sims.top(K + 1)(Ordering.by[(Int, Double), Double] { case (id, similarity) => similarity })
    sortedSims2.slice(1, 11).map { case (id, sim) => (titles(id), sim) }.mkString("\n")
    /*
       String =
       (Canadian Bacon (1994),0.7483422118140968)
       (Airheads (1994),0.7278745617680672)
       (Mystery Science Theater 3000: The Movie (1996),0.7252762702844262)
       (Meet Wally Sparks (1997),0.7140740036831982)
       (Evil Dead II (1987),0.7108942599667418)
       (Army of Darkness (1993),0.7071733229661871)
       (Waiting for Guffman (1996),0.7027410411824709)
       (Body Snatchers (1993),0.7001937255541564)
       (Scream (1996),0.6987844388998511)
       (Nightmare on Elm Street, A (1984),0.6976928817165885)
    */


    /** 评估推荐模型：军方差 K值平均准确率
      *
      * MSE：各平方误差的和 与 总数目 的商。 平方误差是指预测到的评级与真实评级的差值的平方。 */
    // 取789用户的第一个评级-真实评级 预计评级
    val actualRating = moviesForUser.take(1)(0)
    // actualRating: Seq[org.apache.spark.mllib.recommendation.Rating] = WrappedArray(Rating(789,1012,4.0))
    val predictedRating = model.predict(789, actualRating.product)
    // predictedRating: Double = 3.984609159015388

    // 计算平方误差
    val squaredError = math.pow(predictedRating - actualRating.rating, 2.0)
    // squaredError: Double = 2.368779862136182E-4


    /** 计算整个dataset 的Mean Squared Error ,需要对每一条(user, product, actualRating, predictedRating)都计算平方误差，然后求和，再除以总的评级次数 */
    val usersProducts = ratings.map { case Rating(user, product, rating) => (user, product) }
    val predictions = model.predict(usersProducts).map {
      case Rating(user, product, rating) => ((user, product), rating)
    }
    val ratingsAndPredictions = ratings.map {
      case Rating(user, product, rating) => ((user, product), rating)
    }.join(predictions) // 用户-物品 为主键，实际和预计评级为对应值


    val MSE = ratingsAndPredictions.map {
      case ((user, product), (actual, predicted)) => math.pow((actual - predicted), 2)
    }.reduce(_ + _) / ratingsAndPredictions.count
    println("Mean Squared Error = " + MSE)
    // Mean Squared Error = 0.0854164620097481


    val RMSE = math.sqrt(MSE)
    println("Root Mean Squared Error = " + RMSE)
    // Root Mean Squared Error = 0.29226094848567796


    /** Compute Mean Average Precision at K
      * MAPK是指整个数据集上的K值平均准确率APK的均值。
      * APK是信息检索中常用的一个指标，用于衡量针对某个查询所返回的“前K个”文档的平均相关性。文档排名十分重要，同样适合评估隐式数据集的推荐 */

    /* APK评估：每一个用户相当于以个查询，而每一个前K个推荐物组成的集合相当于一个查到的文档的结果集合。试图衡量模型对用户感兴趣和会去接触的物品的预测能力。 */
    // 计算平均准确率的公式  Code for this function is based on: https://github.com/benhamner/Metrics
    def avgPrecisionK(actual: Seq[Int], predicted: Seq[Int], k: Int): Double = {
      val predK = predicted.take(k)
      var score = 0.0
      var numHits = 0.0
      for ((p, i) <- predK.zipWithIndex) {
        if (actual.contains(p)) {
          numHits += 1.0
          score += numHits / (i.toDouble + 1.0) // 命中次数/下标加1
        }
      }
      if (actual.isEmpty) {
        1.0
      } else {
        score / scala.math.min(actual.size, k).toDouble
      }
    }
    // 取出用户789实际评级过的电影的ID
    val actualMovies = moviesForUser.map(_.product)
    // actualMovies: Seq[Int] = ArrayBuffer(1012, 127, 475, 93, 1161, 286, 293, 9, 50, 294, 181, 1, 1008, 508, 284, 1017, 137, 111, 742, 248, 249, 1007, 591, 150, 276, 151, 129, 100, 741, 288, 762, 628, 124)
    // 取出推荐的列表，k为10
    val predictedMovies = topKRecs.map(_.product)
    // predictedMovies: Array[Int] = Array(156, 192, 482, 346, 1019, 23, 201, 479, 603, 269)
    // 计算平均准确率
    val apk10 = avgPrecisionK(actualMovies, predictedMovies, 10)
    // apk10: Double = 0.0 可以看到效果不理想


    /** 计算全局MAPK：计算每一个用户的APK得分，再求其平均。Compute recommendations for all users */

    // 取回物品因子向量并用它来构建一个DoubleMatrix对象
    val itemFactors = model.productFeatures.map { case (id, factor) => factor }.collect()
    val itemMatrix = new DoubleMatrix(itemFactors)
    println(itemMatrix.rows, itemMatrix.columns)
    // (1682,50) - movies数目,因子维度
    val imBroadcast = sc.broadcast(itemMatrix)


    // 计算每一个用户的推荐（预计评级：用户电影因子的点积），之后用预计评级对他们排序
    val allRecs = model.userFeatures.map { case (userId, array) =>
      val userVector = new DoubleMatrix(array)
      val scores = imBroadcast.value.mmul(userVector)
      val sortedWithId = scores.data.zipWithIndex.sortBy(-_._1)
      val recommendedIds = sortedWithId.map(_._2 + 1).toSeq // 返回的电影id需要+1，因为物品矩阵编号从0开始，而电影标号从1开始
      (userId, recommendedIds)
    }


    // next get all the movie ids per user, grouped by user id
    val userMovies = ratings.map { case Rating(user, product, rating) => (user, product) }.groupBy(_._1)
    // userMovies: org.apache.spark.rdd.RDD[(Int, Iterable[(Int, Int)])] = ShuffledRDD[244] at groupBy at <console>:30


    // finally, compute the APK for each user, and average them to find MAPK
    //    val K = 10
    val MAPK = allRecs.join(userMovies).map { case (userId, (predicted, actualWithIds)) =>
      val actual = actualWithIds.map(_._2).toSeq
      avgPrecisionK(actual, predicted, K)
    }.reduce(_ + _) / allRecs.count
    println("Mean Average Precision at K = " + MAPK)
    // Mean Average Precision at K = 0.041147174670504494 推荐类任务得分通常较低，特别是当数量极大时


    /** 使用MLlib内置的评估函数 （MSE, RMSE and MAPK） */


    import org.apache.spark.mllib.evaluation.RegressionMetrics
    val predictedAndTrue = ratingsAndPredictions.map { case ((user, product), (actual, predicted)) => (actual, predicted) }
    val regressionMetrics = new RegressionMetrics(predictedAndTrue)
    println("Mean Squared Error = " + regressionMetrics.meanSquaredError)
    println("Root Mean Squared Error = " + regressionMetrics.rootMeanSquaredError)
    // Mean Squared Error = 0.08541646200974809
    // Root Mean Squared Error = 0.2922609484856779 与之前计算的结果完全一样


    // MAPK
    import org.apache.spark.mllib.evaluation.RankingMetrics
    val predictedAndTrueForRanking = allRecs.join(userMovies).map { case (userId, (predicted, actualWithIds)) =>
      val actual = actualWithIds.map(_._2)
      (predicted.toArray, actual.toArray) //(预测物品IDs，实际物品IDs)
    }
    val rankingMetrics = new RankingMetrics(predictedAndTrueForRanking)
    println("Mean Average Precision = " + rankingMetrics.meanAveragePrecision)
    // Mean Average Precision = 0.08447647846869293\


    // 老方法计算MAPK，将K值设很高时，和上面模型得到的结果相同，上面并不设定阈值K
    val MAPK2000 = allRecs.join(userMovies).map { case (userId, (predicted, actualWithIds)) =>
      val actual = actualWithIds.map(_._2).toSeq
      avgPrecisionK(actual, predicted, 2000)
    }.reduce(_ + _) / allRecs.count
    println("Mean Average Precision = " + MAPK2000)
    // Mean Average Precision = 0.0844764784686929


  }
}