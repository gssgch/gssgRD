package main.scala.com.ch.spark.graphx.sampledemo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Graph, VertexRDD}
import org.apache.spark.graphx.util.GraphGenerators

/**
  *
  * Created by ch on 2017/4/26.
  * Email: 824203453@qq.com
  * Aggregate Messages (aggregateMessages)
  */
object NeighAggsGraph {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]").setAppName("xx")
    val sc: SparkContext = new SparkContext(conf)


    // Create a graph with "age" as the vertex property.  Here we use a random graph for simplicity.
    val graph: Graph[Double, Int] =
    GraphGenerators.logNormalGraph(sc, numVertices = 100).mapVertices((id, _) => id.toDouble)

    // mapVertices 用于生成顶点的属性，这里使用的是顶点值转为double即可，第二个_貌似是个随机double值
    // graph.vertices.foreach(println(_))

    // Compute the number of older followers and their total age
    val olderFollowers: VertexRDD[(Int, Double)] = graph.aggregateMessages[(Int, Double)](
      triplet => {
        // 要发送的消息
        // Map Function
        if (triplet.srcAttr > triplet.dstAttr) {
          // Send message to destination vertex containing counter and age
          triplet.sendToDst(1, triplet.srcAttr)
        }
      },
        // 合并消息
        // Add counter and age
        (a, b) => (a._1 + b._1, a._2 + b._2) // Reduce Function
    )
    // Divide total age by number of older followers to get average age of older followers
    val avgAgeOfOlderFollowers: VertexRDD[Double] =
    olderFollowers.mapValues((id, value) => value match {
      case (count, totalAge) => totalAge / count
    })
    // Display the results
    avgAgeOfOlderFollowers.collect.foreach(println(_))

  }
}
