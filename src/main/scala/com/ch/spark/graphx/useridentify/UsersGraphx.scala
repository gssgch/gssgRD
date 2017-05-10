package com.ch.spark.graphx.useridentify

import java.util.UUID

import main.scala.com.ch.spark.utils.LogsBean
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 不明白的一点：这里为什么导入的包需要main.scala
  * 已经把scala目录设置为源，而且在pom文件中配置了
  */
/**
  * 统一用户识别
  * Created by  on 2016/12/5.
  */
object UsersGraphx {

  def main(args: Array[String]): Unit = {
    // 设置日志级别
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    if (args.length < 2) {
      println(
        """
          |Usage:UsersGraphx <inputFiles> <userShipOutputFiles> <userResultOutputFiles>
        """.stripMargin)
//                  sys.exit(0)
    }

//            val Array(inputFiles, userShipOutputFiles, userResultOutputFiles) = args

    val inputFiles = "F:\\company\\lomark\\平台数据&自动化脚本\\原始数据parquet文件\\2016-10-01_00_p1_invalid.1475254927494.log.20161001012611215.parquet"
    val userShipOutputFiles ="usership.txt"
    val userResultOutputFiles =""

    // 设置SparkConf 及 序列化方式
    val sparkConf = new SparkConf()
    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
//    sparkConf.setMaster("local")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.registerKryoClasses(Array(classOf[LogsBean]))

    // 初始化 sparkcontext
    val sc = new SparkContext(sparkConf)

    // 读取数据文件
    //        val dataSource = sc.textFile(inputFiles)
    val sqlc = new SQLContext(sc)
    sqlc.setConf("spark.sql.parquet.compression.codec", "snappy")

    // 记录着用户的原始数据(imei, imeimd5, ....., UUID)
    val baseDataRdd = sqlc.read.parquet(inputFiles).map {
      line => {
        val logBean = LogsBean.buidFormParquet(line)
        val userIdsSet = makeUserKeyID(logBean)
        if (userIdsSet.size > 0) {
          List[String](math.abs(("UUID:" + UUID.randomUUID()).hashCode).toString) ++ userIdsSet.toList
        } else {
          List[String]()
        }
      }
    }.filter(_.size > 0).distinct().cache()
    // List(UUID:-167768006, IMEIMD5:F8063BA05814AB742E76EB473F8E8EFF, ANDROIDID:96DED21F38D05C47)

//    baseDataRdd.foreach(println)
//    sys.exit(0)

    // 找出当天数据中存在可以合并的用户关系
    baseDataRdd.flatMap(
      ids => ids.slice(1, ids.length).map(t => (t, ids(0)))
    ).reduceByKey((a, b) => a.concat(",") + b).map(_._2)
      .flatMap(
        shipline => {
          val shipFields = shipline.split(",", shipline.length)
          var i = 0
          var k = 0
          val allShips = scala.collection.mutable.ArrayBuffer[(Long, Long)]()
          while (i < shipFields.length) {
            while (k < shipFields.length) {
              allShips += ((shipFields(i).toLong, shipFields(k).toLong))
              k += 1
            }
            i += 1
          }
          allShips
        }
      ).map(t => t._1 + " " + t._2).saveAsTextFile(userShipOutputFiles)
//      .foreach(println)
//    sys.exit(0)
// 1612156872 1612156872
//    1638792447 1638792447


    // 构建边集合
    val graph = GraphLoader.edgeListFile(sc, userShipOutputFiles)
    // 构建连通图的顶点
    val cc = graph.connectedComponents().vertices

    baseDataRdd.map {
      line => (line(0).toString.toLong, line.slice(1, line.length).toSet)
    }.join(cc).map {
      case (id, (userids, cc)) => (cc, userids)
    }.reduceByKey((a, b) => a ++ b).map(
      t => {
        val idStrs = t._2.mkString(",")
        "UID:" + idStrs.hashCode + "," + idStrs
      }
    )
//  .foreach(println)
          .saveAsTextFile(userResultOutputFiles)

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
