package main.scala.com.ch.spark.graphx.lookalike

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * Created by ch on 2017/4/26.
  * Email: 824203453@qq.com
  * 最简单的AggregateMessages Demo
  */
object AggMsgGraphDemo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[1]").setAppName("xx")
    val sc: SparkContext = new SparkContext(conf)

    /**
      * 构建usersRDD
      * 先以一个属性示例，
      */
    // verticesRDD  同时包含顶点和顶点属性
    val users: RDD[(VertexId, String)] =
    sc.parallelize(Array((135L, "tag1"), (136L, "tag2"), (137L, "tag2"), (138L, "tag3")))


    /** 单独加载顶点和顶点的属性， */

    /*
    // 加载顶点
    val activeUsers: RDD[(VertexId,String)] =
      sc.parallelize(Array((135L, null), (136L, null),
        (137L,null), (138L, null)))
    // 加载顶点的属性
    val bindUsers: RDD[(VertexId,String)] =
      sc.parallelize(Array((135L,"t2"), (136L, "t4"),
        (137L,"t5")))
    // 合并生成顶点 可以使用reduceBykey,groupBykey
    val users =bindUsers.union(activeUsers).reduceByKey({(a,b)=>if(!a.isEmpty) a else b})
    val users2 =bindUsers.union(activeUsers).groupByKey().map{
      case (k,v)=>(k,v.head)
    }*/


    // Create an RDD for edges
    val relationships: RDD[Edge[(Int)]] =
    sc.parallelize(Array(Edge(135L, 136L, 5), Edge(136L, 137L, 3),
      Edge(137L, 138L, 6), Edge(138L, 135L, 2), Edge(138L, 137L, 2)))

    // 默认用户设置顶点属性
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("Missing")

    // Build the initial Graph
    //    val graph = Graph(users,relationships)
    /** 如果使用了默认用户，就要在后续过滤掉 */
    //    val graph = Graph(users, relationships, defaultUser)
    val graph = Graph.apply(users, relationships, defaultUser)
    // 过滤掉无效数据
    //      .subgraph(vpred=(id,attr)=>attr!="Missing")

    // 查看顶点
    // Count all users which are postdocs
    graph.vertices.filter { case (id, tag) => tag == "tag2" }.foreach(println)

    // 查看边
    // Count all the edges where src > dst
    //    graph.edges.filter(e => e.srcId > e.dstId).foreach(println)

    // 三元组视图 得到 顶点之间的关系，其中关系是在边中
    // Use the triplets view to create an RDD of facts.
    //    val facts: RDD[String] =
    //    graph.triplets.map(triplet =>
    //      triplet.srcAttr + " is the " + triplet.attr + " of " + triplet.dstAttr)
    //    facts.collect.foreach(println(_))    // 查看顶点

//    graph.pregel(String)(
//      (id, dist, newDist) => dist.length>newDist.length,
//      triplet => {
//        // 要发送的消息
//        Iterator((triplet.dstId, triplet.srcAttr ))
//      },
//      (a,b)=>if (a.isE)
//
//    )

    /**
      * 注意：顶点的属性是可以自己设定的，给一个可以用String，给多个，可以用Array[String],但前后必须对应
      */
    //    val olderFollowers: VertexRDD[Array[String]] = graph.aggregateMessages[Array[String]]
    val olderFollowers: VertexRDD[(String)] = graph.aggregateMessages[(String)](
      triplet => {
        // 要发送的消息
        // Map Function
        //        if (triplet.srcAttr > triplet.dstAttr) {
        // Send message to destination vertex containing counter and age
        triplet.sendToDst(triplet.srcAttr)
        //          triplet.sendToSrc(triplet.dstAttr)
        //        }
      },
      // 合并消息
      // Add counter and age
      (a, b) => (a + b) // Reduce Function
      // 指定传递哪些属性
      , tripletFields = TripletFields.Dst
      //      ,tripletFields = TripletFields.Src
    )
    // 如果上面设置只传递 dst，那么srcAttr就没有值了，send后的输出结果，attr都是null
    // 那么反之，如果设置只传递src，那么用sendToSrc,结果一样是null
//    olderFollowers.foreach(println)

    // Divide total age by number of older followers to get average age of older followers
    val avgAgeOfOlderFollowers: VertexRDD[String] =
    olderFollowers.mapValues((id, value) => value)

    /*  avgAgeOfOlderFollowers.join(users).map{
        x=>
          (x._1,x._2._1+" ".concat(x._2._2))
      }.foreach(println(_))*/
    // Display the results
    //    avgAgeOfOlderFollowers.collect.foreach(println(_))

  }


}
