package main.scala.com.ch.spark.graphx.useridentify

import java.util.UUID

import main.scala.com.ch.spark.utils.LogsBean
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 统一用户识别
  * 利用graphx 连通图实现统一用户识别
  * Created by  on 2016/12/5.
  */
object UsersGraphCC {

  def main(args: Array[String]): Unit = {
    // 设置日志级别
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    if (args.length < 2) {
      println(
        """
          |Usage: UsersGraphx <inputFiles> <userShipOutputFiles> <userResultOutputFiles>
        """.stripMargin)
      //      sys.exit(0)
    }

    //    val Array(inputFiles, userShipOutputFiles, userResultOutputFiles) = args
    val inputFiles = "F:\\company\\juxiang\\data\\parquet\\part-r-00012.gz.parquet"
    val userShipOutputFiles = "shipfile.txt"
    val userResultOutputFiles = "out.txt"

    // 设置SparkConf 及 序列化方式
    val sparkConf = new SparkConf()
    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.setMaster("local[1]")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.registerKryoClasses(Array(classOf[LogsBean]))

    // 初始化 sparkcontext
    val sc = new SparkContext(sparkConf)

    // 读取数据文件
    //        val dataSource = sc.textFile(inputFiles)
    val sqlc = new SQLContext(sc)
    sqlc.setConf("spark.sql.parquet.compression.codec", "snappy")

    // 记录着用户的原始数据(imei, imeimd5, ....., UUID)
    val baseDataRdd = sqlc.read.parquet(inputFiles).coalesce(1).map {
      line => {
        val LogBean = LogsBean.buidFormParquet(line)
        val userIdsSet = makeUserKeyID(LogBean)
        if (userIdsSet.size > 0) {
          List[String](math.abs(("UUID:" + UUID.randomUUID()).hashCode).toString) ++ userIdsSet.toList
        } else {
          List[String]()
        }
      }
    }.filter(_.size > 0).distinct().cache()
    // List(UUID:-167768006, IMEIMD5:F8063BA05814AB742E76EB473F8E8EFF, ANDROIDID:96DED21F38D05C47)

    baseDataRdd.foreach(println)
    println("-------------以上是原始数据------------------")

    println("-------------以下是新开发------------")
    baseDataRdd.flatMap {
      case k =>
        k.tail.map(v => (v, k.head))
    }
      /* baseDataRdd.map {
         case k => (k.head, k.tail)
       }.flatMap {
         case (k1, v1) => v1.map(v2 => (v2, k1))
       }*/
      .reduceByKey((a, b) => a.concat(",") + b)
      // .map(_._2) 针对每一个id标识，获取关联上的uid集合
      .map(_._2)
      .flatMap {
        ship =>
          var allShip = ship.split(",", -1)
          var nums:Set[Tuple2[Long,Long]] = Set()
          for (i <- 0 until allShip.size) {
            for (j <- i until allShip.size) {
              nums = nums ++ Set((allShip(i).toLong,allShip(j).toLong))
            }
          }
          nums
      }
      .map(t => t._1 + " " + t._2).saveAsTextFile(userShipOutputFiles + "2")

    //      .foreach(println)
    // 下面这种方式生成的图，顶点的属性将被设置为一个默认值
    val graph2 = GraphLoader.edgeListFile(sc, userShipOutputFiles + "2")
    // 构建连通图的顶点
    val cc2 = graph2.connectedComponents().vertices

    // 这里的顶点就是具体的顶点，这里的cc就是联通图的标识
    cc2.foreach(println) // (vertices cc)

    baseDataRdd.map { x => (x.head.toLong, x.tail.toSet) }
      .join(cc2)
      .map {
        case (_, (tails, cc2)) =>
          (cc2, tails)
      }.reduceByKey(_ ++ _)
      .map { x =>
        "UID:" + x._1 + "," + x._2.mkString(",")
      }
      .saveAsTextFile(userResultOutputFiles + "2")

    sc.stop()
  }

  /**
    * 提起单条数据中的用户关键标识
    */
  def makeUserKeyID(log: LogsBean) = {
    val userIds = scala.collection.mutable.Set[String]()
    // userIds ++= Set("UUID:"+UUID.randomUUID())
    if (log.imei.nonEmpty && (log.imei.length == 14 || log.imei.length == 15)) {
      userIds ++= Set("IMEI:" + log.imei.substring(0, 14).toUpperCase)
    }
    if (log.imeimd5.nonEmpty && log.imeimd5.length == 32) {
      userIds ++= Set("IMEIMD5:" + log.imeimd5.toUpperCase)
    }
    if (log.imeisha1.nonEmpty && log.imeisha1.length == 40) {
      userIds ++= Set("IMEISHA1:" + log.imeisha1.toUpperCase)
    }
    if (log.mac.nonEmpty && (log.mac.length == 12 || log.mac.length == 17)) {
      userIds ++= Set("MAC:" + log.mac.toUpperCase)
    }
    if (log.macmd5.nonEmpty && log.macmd5.length == 32) {
      userIds ++= Set("MACMD5:" + log.macmd5.toUpperCase)
    }
    if (log.macsha1.nonEmpty && log.macsha1.length == 40) {
      userIds ++= Set("MACSHA1:" + log.macsha1.toUpperCase)
    }
    if (log.idfa.nonEmpty && (log.idfa.length == 32 || log.idfa.length == 36)) {
      userIds ++= Set("IDFA:" + log.idfa.toUpperCase)
    }
    if (log.idfamd5.nonEmpty && log.idfamd5.length == 32) {
      userIds ++= Set("IDFAMD5:" + log.idfamd5.toUpperCase)
    }
    if (log.idfasha1.nonEmpty && log.idfasha1.length == 40) {
      userIds ++= Set("IDFASHA1:" + log.idfasha1.toUpperCase)
    }
    if (log.openudid.nonEmpty) {
      userIds ++= Set("OPENUDID:" + log.openudid.toUpperCase)
    }
    if (log.openudidmd5.nonEmpty && log.openudidmd5.length == 32) {
      userIds ++= Set("OPENDUIDMD5:" + log.openudidmd5.toUpperCase)
    }
    if (log.openudidsha1.nonEmpty && log.openudidsha1.length == 40) {
      userIds ++= Set("OPENUDIDSHA1:" + log.openudidsha1.toUpperCase)
    }
    if (log.androidid.nonEmpty) {
      userIds ++= Set("ANDROIDID:" + log.androidid.toUpperCase)
    }
    if (log.androididmd5.nonEmpty && log.androididmd5.length == 32) {
      userIds ++= Set("ANDROIDIDMD5:" + log.androididmd5.toUpperCase)
    }
    if (log.androididsha1.nonEmpty && log.androididsha1.length == 40) {
      userIds ++= Set("ANDROIDIDSHA1:" + log.androididsha1.toUpperCase)
    }
    userIds
  }

}
