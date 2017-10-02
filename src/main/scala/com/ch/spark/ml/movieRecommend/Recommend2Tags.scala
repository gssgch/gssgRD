package main.scala.com.ch.spark.ml.movieRecommend

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.jblas.DoubleMatrix

/**
  * Hubeimobile  推荐标签
  *
  * 使用电影的测试数据
  * userid不超过1000
  * movieid不超过700
  *
  */

object Recommend2Tags {
  val K = 10

  Logger.getLogger("org.apache.spark").setLevel(Level.INFO)
  Logger.getLogger("org.apache.eclipse.jetty.server").setLevel(Level.OFF)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.eclipse.jetty.server").setLevel(Level.OFF)

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local").setAppName("tagRecommend")
    val sc = new SparkContext(conf)



    //    extractTestData(sc)


    /** */
    // 提取特征  用户   套餐  权重


    val file = "F:\\company\\lomark\\Hubei10086\\recommendData/orignal.log"
    val file2 = "F:\\company\\lomark\\Hubei10086\\recommendData/taocan.txt"
    /** 提取特征(影片 ID 星级 事件戳) */
    val rawData = sc.textFile(file)
    val ratings = rawData.map(_.split(",")).map {
      case Array(user, keys, rating)
      =>
        Rating(user.toInt, keys.toInt, rating.toDouble)
    }

    /** 训练推荐模型 */
    val rank = 50 //因子个数
    val iteratings = 10 //迭代
    val lambda = 0.01 //正则化参数

    // 推荐给所有用户的方法
    ALS.train(ratings, rank, iteratings, lambda)
      .recommendProductsForUsers(3)
//      .foreach(println)
      .map{
      case (hash,top)=>{
        println("key="+hash)
        val ids = top.map { case Rating(user, product, rating)=>product}
            .mkString(",")
        (hash,ids)
      }
    }
//      .map(_._2)
      .foreach(println)
    sys.exit(0)


    // 使用ALS建立模型
    val model = ALS.train(ratings, rank, iteratings, lambda)

    /** predict函数方便的计算 给定用户对给定的预期评分 */
    val predictScore = model.predict(789, 123)
    println("predictScore=" + predictScore)

    /** 使用推荐模型 为指定用户生成前k个推荐 */
    val userID = 789
    val K = 10
    val topKRecs = model.recommendProducts(userID, K)
    println(topKRecs.mkString("\n")) // Rating(789,19,3.9724141049970996)
    sc.makeRDD(topKRecs).map {
      case Rating(uid, tid, rate) => {
        uid + "," + tid + "," + rate
      }
    }
      .foreach(println)
    //          .saveAsTextFile("")

    /** 检验推荐内容 */
    //    val titles = sc.textFile(file2)
    //      .map(_.split(",")).map(array => (array(0).toInt, array(1))).collectAsMap()

    //    printf("给用户 " + userID + "推荐的前" + K + " 套餐如下\n")
    //    topKRecs.map(rating => (titles(rating.product), rating.rating)).foreach(println)

    //    sys.exit(0)


    /** 效果评估 */
    val usersProducts = ratings.map { case Rating(user, product, rating) => (user, product) }
    val predictions = model.predict(usersProducts).map {
      case Rating(user, product, rating) => ((user, product), rating)
    }
    val ratingsAndPredictions = ratings.map {
      case Rating(user, product, rating) => ((user, product), rating)
    }.join(predictions) // 用户-物品 为主键，实际和预计评级为对应值

    import org.apache.spark.mllib.evaluation.RegressionMetrics
    val predictedAndTrue = ratingsAndPredictions.map { case ((user, product), (actual, predicted)) => (actual, predicted) }
    val regressionMetrics = new RegressionMetrics(predictedAndTrue)
    println("Mean Squared Error = " + regressionMetrics.meanSquaredError)
    println("Root Mean Squared Error = " + regressionMetrics.rootMeanSquaredError)



    /** 计算全局MAPK：计算每一个用户的APK得分，再求其平均。Compute recommendations for all users */

    // 取回物品因子向量并用它来构建一个DoubleMatrix对象
    val itemFactors = model.productFeatures.map { case (id, factor) => factor }.collect()
    val itemMatrix = new DoubleMatrix(itemFactors)
    val imBroadcast = sc.broadcast(itemMatrix)
    // 计算每一个用户的推荐（预计评级：用户电影因子的点积），之后用预计评级对他们排序
    val allRecs = model.userFeatures.map { case (userId, array) =>
      val userVector = new DoubleMatrix(array)
      val scores = imBroadcast.value.mmul(userVector)
      val sortedWithId = scores.data.zipWithIndex.sortBy(-_._1)
      val recommendedIds = sortedWithId.map(_._2 + 1).toSeq // 返回的套餐id需要+1，因为物品矩阵编号从0开始，而套餐标号从1开始
      (userId, recommendedIds)
    }

    val userMovies = ratings.map { case Rating(user, product, rating) => (user, product) }.groupBy(_._1)
    // MAPK
    import org.apache.spark.mllib.evaluation.RankingMetrics
    val predictedAndTrueForRanking = allRecs.join(userMovies).map { case (userId, (predicted, actualWithIds)) =>
      val actual = actualWithIds.map(_._2)
      (predicted.toArray, actual.toArray) //(预测物品IDs，实际物品IDs)
    }
    val rankingMetrics = new RankingMetrics(predictedAndTrueForRanking)
    println("Mean Average Precision = " + rankingMetrics.meanAveragePrecision)


  }

  /** 从电影日志里抽取出部分测试数据 */
  def extractTestData(sc: SparkContext): Nothing = {
    // 生成测试数据-用户
    val file1 = "F:\\company\\lomark\\HubeiMobile\\recommendData/ratings.dat"
    sc.textFile(file1)
      .map(_.split("::").take(3)).filter(x => x(0).toInt < 1000).sample(false, 0.1, 0)
      .map {
        case Array(u, mid, r) =>
          if (mid.toInt >= 700) {
            Array(u, mid.toInt / 100, r)
          } else {
            Array(u, mid, r)
          }
      }.map(_.mkString(",")).foreach(println)
    sys.exit(0)

    val file3 = "F:\\company\\lomark\\HubeiMobile\\recommendData/movies.dat"
    sc.textFile(file3)
      .map(line => line.split("::").take(2)).filter(_ (0).toInt < 700)
      .map({
        case Array(id, name) => {
          Array(id, "taocan".concat(id)).mkString(",")
        }
      })
      .foreach(println(_))
    sys.exit(0)
  }
}