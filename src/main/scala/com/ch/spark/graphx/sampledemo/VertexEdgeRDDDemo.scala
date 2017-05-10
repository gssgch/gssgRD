package main.scala.com.ch.spark.graphx.sampledemo

import org.apache.spark.graphx.{VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * Created by ch on 2017/5/4.
  * Email: 824203453@qq.com
  * VertexRDD 和 EdgeRDD
  */
object VertexEdgeRDDDemo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[2]").setAppName("xx")
    val sc = new SparkContext(conf)

    val setA: VertexRDD[Int] = VertexRDD(sc.parallelize(0L until 100L).map(id => (id, 1)))
    val rddB: RDD[(VertexId, Double)] = sc.parallelize(0L until 100L).flatMap(id => List((id, 1.0), (id, 2.0)))
    // There should be 200 entries in rddB
    rddB.count
    val setB: VertexRDD[Double] = setA.aggregateUsingIndex(rddB, _ + _)
    // There should be 100 entries in setB
    setB.count
    // Joining A and B should now be fast!
    val setC: VertexRDD[Double] = setA.innerJoin(setB)((id, a, b) => a + b)
  }

}
