package main.scala.com.ch.spark.graphx.lookalike

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * Created by ch on 2017/4/26.
  * Email: 824203453@qq.com
  * 读取hive表，构建用户图及关系图
  * 可能微厅渠道爱好的人
  */
object AggMsgGraph {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println(
        """|Usage: <sourceTable> <outTable> <stat_date>
          |eg: lmbigdata.hubeilog /adlogs/hubei/recommendout/out2/""".stripMargin)
//      sys.exit(0)
    }
    val Array(activeTable, bindTable,friendsTable, stat_date,outPath) = args

    val conf = new SparkConf()
    //      .setMaster("local[1]").setAppName("xx")
    val sc = new SparkContext(conf)
    val sqlc = new HiveContext(sc)

    /**
      * 构建usersRDD
      */
    val activeUsers = sqlc.sql("select distinct phone_no from " + activeTable +
      " where stat_month=" + stat_date.substring(0, 6))
      .map(x => (x.getString(0).toLong, ""))

    val bindUsers = sqlc.sql("select distinct phone_no from " + bindTable +
      " where user_type=1 and stat_date=" + stat_date)
      .map(x => (x.getString(0).toLong, "1"))

    //    val activeUsers: RDD[(VertexId, String)] =
    //    sc.parallelize(Array((135L, ""), (136L, ""),
    //      (137L, ""), (138L, ""), (139L, ""), (140L, ""), (141L, "")))
    //    val bindUsers: RDD[(VertexId, String)] =
    //      sc.parallelize(Array((135L, "t2"), (136L, "t4"),
    //        (137L, "t5")))
    val users:RDD[(VertexId, String)] = bindUsers.union(activeUsers)
      .reduceByKey({ (a, b) => if (!a.isEmpty) a else b })
      .cache()



    // Create an RDD for edges
//    val relationships: RDD[Edge[(Int)]] =
//    sc.parallelize(Array(Edge(135L, 136L, 5), Edge(136L, 137L, 3), Edge(136L, 140L, 3),
//      Edge(137L, 138L, 6), Edge(137L, 139L, 2), Edge(135L, 139L, 2)))

    val relationships = sqlc.sql("select * from " + friendsTable +
      " where  stat_date=" + stat_date)
      .map{row =>
        val phone_no = row.getString(0)
        val call_no = row.getString(1)
        val weight = row.getInt(2)
        //        Edge(phone_no.toLong,call_no.toLong,weight)
        (phone_no, call_no, weight)
      }.filter(_._2.matches("\\d{11}"))
      .map {
        case (phone_no, call_no, weight) => {
          Edge(phone_no.toLong, call_no.toLong, weight)
        }
      }

    // Build the initial Graph
    val graph = Graph(users, relationships)
//   val anotherGraph= Graph.apply(users,relationships)

    val msgUsers = graph.aggregateMessages[String](
      triplet => {
        // Map Function src是绑定用户，且dst是未绑用户  才会发送消息
        if (!triplet.srcAttr.isEmpty && triplet.dstAttr.isEmpty) {
          triplet.sendToDst(triplet.srcAttr)
        }
      },
      // 合并消息 这里接收推荐了多个时，只取一个就行了
      (a, b) => if (!a.isEmpty) a else b
      ,tripletFields = TripletFields.All
    )
    //    val newUesrs: VertexRDD[String] =
    //      aggsUsers.mapValues((id, value) => value)

    users.union(msgUsers).reduceByKey { (a, b) => if (!a.isEmpty) a else b }.saveAsTextFile(outPath)

    println("activeUser="+activeUsers.count())
    println("User="+users.count())
    println("msgUser"+msgUsers.count())
  }
}
