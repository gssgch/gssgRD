package main.scala.com.ch.spark.graphx.sampledemo
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
// Import random graph generation library
import org.apache.spark.graphx.util.GraphGenerators
/**
  *
  * Created by ch on 2017/5/4.
  * Email: 824203453@qq.com
  */
object PregelDemo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[2]").setAppName("xx")
    val sc = new SparkContext(conf)

    // A graph with edge attributes containing distances
    val graph: Graph[Long, Double] =
    GraphGenerators.logNormalGraph(sc, numVertices = 100).mapEdges(e => e.attr.toDouble)
    val sourceId: VertexId = 42 // The ultimate source
    // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
      triplet => {  // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a,b) => math.min(a,b) // Merge Message
    )
    println(sssp.vertices.collect.mkString("\n"))
  }

}
