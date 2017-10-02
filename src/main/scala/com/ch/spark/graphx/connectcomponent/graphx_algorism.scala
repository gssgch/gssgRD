package main.scala.com.ch.spark.graphx.connectcomponent

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{GraphLoader, PartitionStrategy}

/**
  *
  * Created by ch on 2017/6/11.
  * Email: 824203453@qq.com
  * spark graphx connected Components   连通图
  */
object graphx_algorism {

  def main(args: Array[String]): Unit = {

    val followersFile = "D:\\workspace\\GitHub\\gssgRD\\src\\main\\scala\\com\\ch\\spark\\graphx/followers.txt"
    val usersFile = "D:\\workspace\\GitHub\\gssgRD\\src\\main\\scala\\com\\ch\\spark\\graphx/users.txt"

    val conf = new SparkConf().setMaster("local[2]").setAppName("graph_algorism").set("spark.cores.max", "10") //set spark.cores.max　可以设置核数
    val sc = new SparkContext(conf)

    // graph初始化，从文件中读
    val graph = GraphLoader.edgeListFile(sc, followersFile)
    val users = sc.textFile(usersFile).map {
      line =>
        val fields = line.split(",")
        (fields(0).toLong, fields(1))
    }


    // 1.
    //PageRank
    val ranks = graph.pageRank(0.001).vertices // 0.001 是PageRank 的参数，尚未知道是什么意思
    ranks.collect.foreach(println)
    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }
    println(ranksByUsername.collect().mkString("\n"))


    //2.
    // Connected Components: LianTongTi
    val cc = graph.connectedComponents().vertices
    println(cc.collect)
    val ccByUsername = users.join(cc).map {
      case (id, (username, cc)) => (username, cc)
    }
    println(ccByUsername.collect().mkString("\n"))


    //3.
    //Triangle Count
    val graphT = GraphLoader.edgeListFile(sc, followersFile, true).partitionBy(PartitionStrategy.RandomVertexCut)
    val triCounts = graphT.triangleCount().vertices
    val triCountByUsername = users.join(triCounts).map { case (id, (username, tc)) => (username, tc) }
    println(triCountByUsername.collect().mkString("\n"))

  }

}
