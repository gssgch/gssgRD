package com.lomark.tools

import java.io.{File, PrintWriter}

import scala.io.Source

/**
  * Created by Jiawei on 16/6/23.
  * 计算AppName的相似度
  */
object EditDistance {

  def levenshtein(str1: String, str2: String): Float = {
    val len1 = str1.trim.length
    val len2 = str2.trim.length

    if (len1 == len2 && str1.trim.equalsIgnoreCase(str2.trim)) {
      1.0f
    } else {
      val editDistanceValue: Float = if (len1 == 0 || len2 == 0) {
        0.0f
      } else {
        var matrix = Array.ofDim[Int](len1 + 1, len2 + 1)
        // 初始化i=0 和 j=0的值
        for (i <- 0 to len1) {
          matrix(i)(0) = i
        }
        for (j <- 0 to len2) {
          matrix(0)(j) = j
        }
        var isSame: Int = 0
        for (i <- 1 to len1) {
          for (j <- 1 to len2) {
            if (str1.charAt(i - 1).toLower.equals(str2.charAt(j - 1))) {
              isSame = 0
            } else {
              isSame = 1
            }
            matrix(i)(j) = Math.min(matrix(i - 1)(j - 1) + isSame, Math.min(matrix(i - 1)(j) + 1, matrix(i)(j - 1) + 1))
          }
        }
        (1.0 - matrix(len1)(len2).toFloat / Math.max(len1, len2).toFloat).toFloat
      }
      editDistanceValue
    }
  }

  def main(args: Array[String]) {
    printf("" + levenshtein("ALISSP-驾校一点通安卓|驾校一点通安卓练习页banner", "驾校一点通_3708"))

//    val lines = Source.fromFile("/Users/Jiawei/Documents/appname副本.txt", "GB18030").getLines().toList
//    val writer = new PrintWriter(new File("/Users/Jiawei/Documents/appnameResult.txt"))
//    for (i <- 0 until lines.size - 1) {
//      println("计算第"+i+"行相似度......")
//      // 记录相似度高的Top5
//      var similarityList: List[Float] = List()
//      var similarityMap = scala.collection.mutable.Map[String, String]()
//      for (j <- (i+1) until lines.size) {
//        val similarity: Float = levenshtein(lines(i), lines(j))
//        similarityList = similarityList.::(similarity)
//
//        if (similarityMap.contains(similarity.toString)) {
//          val newValue = similarityMap.get(similarity.toString) + "," + lines(j)
//          similarityMap.updated(similarity.toString, newValue)
//        } else {
//          similarityMap += (similarity.toString -> lines(j))
//        }
//        similarityList = similarityList.sortWith(_ > _).take(5)
//      }
//
//      // 写出
//      val similarApp = similarityList.filter(_ > 0.6f).map(key => {
//        similarityMap.getOrElse(key.toString, Nil)
//      }).mkString(",")
//
//      writer.write(lines(i)+"\t"+similarApp+"\n")
//    }
//    writer.close()
  }

}