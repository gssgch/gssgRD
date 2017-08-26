package main.scala.com.ch.spark.graphx.sampledemo

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ch on 2017/4/26.
  * Email: 824203453@qq.com
  * graph 构建图
  */
object GraphBuildDemo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[2]").setAppName("xx")
    val sc: SparkContext = new SparkContext(conf)
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof"))))
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(2L, 7L, "collab"), Edge(5L, 3L, "advisor"),
        Edge(7L, 5L, "colleague")))

    // Build the initial Graph
    val graph = Graph(users, relationships) // Graph.apply(users,relationships)



    val graph2 = GraphLoader.edgeListFile(sc,path="")


//    val graph3 = Graph.fromEdges(relationships)

//    val graph3 = Graph.fromEdgeTuples(relationships)
    /** Information about the Graph
      * 顶点的入度 出度 和 所有的度
      * 不论是否有默认用户，顶点数是根据边来计算的，而不管是否真的出现在顶点RDD中
      * */
    val nvs = graph.numVertices
    val nes = graph.numEdges
    // 顶点的入度数（就是顶点被指向的次数） 顶点-入度次数
    val ids = graph.inDegrees // Use the implicit GraphOps.inDegrees operator
    // 顶点的出度数（就是顶点指向出去的次数） 顶点-出度次数
    val ods = graph.outDegrees
    // 顶点所有的度，入度和出度的和
    val dgs = graph.degrees

    //    println(ids.max())

    println("---顶点数=" + nvs + ";;边数=" + nes)
    //        ids.foreach(println)
    //    ods.foreach(println)
    //    dgs.foreach(println)


    /** Views of the graph as collections
      * 以集合方式查看图
      * 顶点图 边图 三元组试图
      * */
    // 查看顶点   // Count all users which are postdocs
    graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" } //.foreach(println)
    // 查看边    // Count all the edges where src > dst
    graph.edges.filter(e => e.srcId > e.dstId) //.foreach(println)
    // 三元组视图 得到 顶点之间的关系，其中关系是在边中
    // Use the triplets view to create an RDD of facts.
    val facts: RDD[String] =
    graph.triplets.map(triplet =>
      triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
    //    facts.collect.foreach(println(_))

    // rxin is the collab of jgonzal ... ...
    // franklin is the advisor of rxin

    /** Functions for caching graphs
      * 图的缓存
      * */
    //    graph.cache()
    //    graph.persist(StorageLevel.MEMORY_ONLY)
    //    graph.unpersist(true)
    //    graph.unpersistVertices(true)

    /** Change the partitioning heuristic
      * 更改分区
      */
    //  graph.partitionBy(PartitionStrategy.EdgePartition1D).edges.foreach(println)
    //  graph.partitionBy(PartitionStrategy.EdgePartition2D).edges.foreach(println)


    /** Transform vertex and edge attributes
      * 转换操作：转换顶点和边属性
      *
      *
      */
    // 改变顶点的属性，可直接改变属性类型，生成新的图   下例中属性类型从(string,string)=> double
    graph.mapVertices((id, attr) => if (id > 6) 6.1 else 5L)
    //    .foreach(println)

    // 改变边的属性，可操作，可改变类型
    graph.mapEdges(e => e.attr.substring(0, 2))
    graph.mapEdges(e => if (e.srcId > e.dstId) e.attr else e.attr.substring(0, 2))
    //      .edges.foreach(println)

    // 改变三元组视图的属性  针对每个triplet进行map操作
    //    graph.mapTriplets(triplet=> triplet.dstId>triplet.srcId)
    graph.mapTriplets(triplet => {
      if (triplet.dstAttr._2 == "Missing") triplet.attr else "xx"
    })
    //      .triplets.foreach(println)
    // 操作完triplet后，还可以再改变顶点属性
    //      .mapVertices((id,_)=>1.0).vertices.foreach(println)


    /** Modify the graph structure
      * 结构操作：改变图结构，生成新的图
      * */
    // reverse 反转所有的边
    graph.reverse
    //      .edges.foreach(println)

    // subgraph 返回满足条件的子图 这里的条件参数2个，第一个是边条件，第二个是顶点条件
    // 如果不设置，默认为全部满足。
    // vprep和 eprep是参数关键字，如果用这种格式(epred=et=>  ,vpred=(id,prop)=> )，必须是epred和vpred
    // 如果不用关键字，可以任意参数名
    graph.subgraph(edge => true, (id, prop) => prop._1.length > 3)
    graph.subgraph(vpred = (id, prop) => prop._1.length > 3)
    graph.subgraph(epred = (et) => et.attr.length > 2)
    graph.subgraph(epred = et => et.attr.length > 2)
    graph.subgraph(et => et.attr.length > 2)
    graph.subgraph(epred => epred.attr.length > 2, (k, v) => true)
    val newGraph = graph.subgraph(epred => epred.dstId > 5)
    //      .edges.foreach(println)

    // mask :返回graph和其他graph的交集
    //    graph.mask(newGraph).edges.foreach(println)


    // groupEdges 将两个顶点之间的多个边合并为一个边   为了保证准确性，先使用 partitionBy对该图进行分区
    //    graph.partitionBy(PartitionStrategy.EdgePartition2D).groupEdges((x,u)=>x+"-"+u).edges.foreach(println)
    //    Edge(8L, 9L, "pi"), Edge(8L, 9L, "pi2")
    //    Edge(8,9,pi-pi2)

    /** Join RDDs with the graph
      * 关联操作：和其他图join
      */
    // joinVertices 第一个参数是聚合的图，第二个参数是函数
    // graph和other相交的顶点执行map函数,在graph中但是不在other中的地点保持不变
    graph.joinVertices(users)((id, vd, _) => vd).edges //.foreach(println(_))

    //outerJoinVertices 和joinVertices类似，但不同的是在graph中但不在other中的顶点也要执行map函数
    // Given a graph where the vertex property is the out degree
    val inputGraph: Graph[Int, String] =
    graph.outerJoinVertices(graph.outDegrees)((vid, _, degOpt) => degOpt.getOrElse(0))
    //      inputGraph.edges.foreach(println)
    // Construct a graph where each edge contains the weight
    // and each vertex is the initial PageRank
    val outputGraph: Graph[Double, Double] =
    inputGraph.mapTriplets(triplet => 1.0 / triplet.srcAttr).mapVertices((id, _) => 1.0)


    /**
      * Aggregate information about adjacent triplets
      * 聚合操作
      *
      */
    // 收集每个顶点的邻居顶点的顶点id和顶点属性。返回的是一个数组
    // in:入度，即指向这个顶点的
    // out:出度，这个顶点指向出去的
    // Either: or的关系
    // Both: and的关系 这个实际运行时报错
    graph.collectNeighbors(EdgeDirection.In)
    //      .collect().foreach(x=>println(x._1+" has nei "+x._2.mkString(",")))
    // 收集每个顶点的邻居顶点的顶点id
    graph.collectNeighborIds(EdgeDirection.Out)
    //      .collect().foreach(x=>println(x._1+" has nei "+x._2.mkString(",")))

    // 消息传递并合并消息
    //    graph.aggregateMessages(ec=>{},(a,b)=>a)


    /** Iterative graph-parallel computation
      * 迭代图并行计算
      */
    //  graph.pregel()
    /** Basic graph algorithms
      * 基本图算法
      */

    // 网页排名
    //    graph.pageRank()
    // 连接组件
    //    graph.connectedComponents
    // 三角计数
    //    graph.triangleCount()
  }
}
