package main.scala.com.ch.spark.graphx.lookalike

import org.apache.spark.graphx.{Edge, Graph, TripletFields, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * Created by ch on 2017/4/26.
  * Email: 824203453@qq.com
  * 根据10086业务需求，本地化数据代码实现
  */
object AggMsgGraphLocalTest {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[1]").setAppName("xx")
    val sc = new SparkContext(conf)

    /**
      * 构建usersRDD
      */
    val activeUsers: RDD[(VertexId, String)] =
    sc.parallelize(Array((135L, ""), (136L, ""),
      (137L, ""), (138L, ""), (139L, ""), (140L, ""), (141L, "")))
    val bindUsers: RDD[(VertexId, String)] =
      sc.parallelize(Array((135L, "t2"), (136L, "t4"),
        (137L, "t5")))
    val users:RDD[(VertexId, String)] = bindUsers.union(activeUsers)
        .groupByKey().map{case (k,v)=>(k,v.head)}
//      .reduceByKey({ (a, b) => if (!a.isEmpty) a else b })

    // Create an RDD for edges
    val relationships: RDD[Edge[(Int)]] =
    sc.parallelize(Array(Edge(135L, 136L, 5), Edge(136L, 137L, 3), Edge(136L, 140L, 3),
      Edge(137L, 138L, 6), Edge(137L, 139L, 2), Edge(135L, 139L, 2),
      Edge(142L, 143L, 2), Edge(137L, 145L, 2), Edge(135L, 148L, 2), Edge(137L, 139L, 2)
    ))


    // Build the initial Graph
    val graph = Graph(users, relationships)

    val msgUsers = graph.aggregateMessages[String](
      triplet => {
        // Map Function src是绑定用户，且dst是未绑用户  才会发送消息
        if (triplet.srcAttr!=null && triplet.dstAttr!=null &&
          !triplet.srcAttr.isEmpty && triplet.dstAttr.isEmpty) {
          triplet.sendToDst(triplet.srcAttr)
        }
      },

      // 合并消息 这里接收推荐了多个时，只取一个就行了
      (a, b) => if (!a.isEmpty) a else b
//      (a, b) => a ++ b
//      ,tripletFields = TripletFields.All
    )
    println("msgUsers.count==="+msgUsers.count())

    msgUsers.foreach(println)

    users.union(msgUsers)
//      .groupByKey().map{case (k,v)=>(k,v.head)}
      // 注意这里不能直接使用v.head 会导致扩散的标签丢失了
      .groupByKey().map{case (k,v)=>(k,v.toSeq.sortWith(_.length>_.length)(0))}
//      .reduceByKey { (a, b) => if (!a.isEmpty) a else b }
      .foreach(println(_))

  }
}
