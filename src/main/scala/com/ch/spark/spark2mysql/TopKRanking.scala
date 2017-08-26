package main.scala.com.ch.spark.spark2mysql

/**
  * Created by ch on 2017/2/22 0001.
  * spark写入mysql的几种方法
  * 以及spark读取文件的几种方法
  */

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.net.URI
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.{Date, Properties}
import java.util.regex.Pattern

import main.scala.com.ch.spark.utils.LogsBean
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object TopKRanking {

  private val starRolePath = "hdfs://dsdc04:8020/adlogs/applibs/starRole.txt"
  private val moviePath = "hdfs://dsdc04:8020/adlogs/applibs/tvName.txt"


  //  case class Ranking(name: String, date: String, counts: Int)

  // 写mysql的方法1
  def fun(iterator: Iterator[(String, Int)]): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "insert into tags_ranking(name,type,date,counts) values (?,?,?,?)"
    try {
      //      Class.forName("com.mysql.jdbc.Driver")
      conn = DriverManager.getConnection("jdbc:mysql://101.200.87.96:3306/dmpdb?useUnicode=true&characterEncoding=utf8", "dmper", "Dmp@donson.com1234")
      //      conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/test?useUnicode=true&characterEncoding=utf8", "root", "123")
      // amp;
      iterator.foreach {
        iter =>
          val data = iter._1.split(" ")
          if (data.size != 3) {
            println("=====xxx=======", iter._1, data.toList)
          }
          ps = conn.prepareStatement(sql)
          ps.setString(1, data(0))
          ps.setInt(2, data(1).toInt)
          ps.setString(3, data(2))
          ps.setInt(4, iter._2)
          ps.executeUpdate()
      }

    } catch {
      case e: Exception =>
        println("------mysql exception", e)
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage:MakeTags <inputLogBase> <outputPath>") /*   <partition>*/
      System.exit(0)
    }
    val Array(inputLogBase, outputPath /*, pars*/) = args

    //            val masterId = "local[2]"
    //            val inputLogBase = "F:\\company\\lomark\\平台数据&自动化脚本\\原始数据parquet文件\\2016-10-01_00_p1_invalid.1475254927494.log.20161001012611215.parquet"


    val conf = new SparkConf()
    //      .setAppName("topKRanking")
    //        .setMaster(masterId)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[LogsBean]))


    // load data from hdfs
//    val list = loadDataFromHDFS(starRolePath) ++ loadDataFromHDFS(moviePath)
//    var starList = loadDataFromHDFS(starRolePath)
//    var tvList = loadDataFromHDFS(moviePath)

    // load data local test
//    val starList = Source.fromFile("src/main/scala/com/lomark/hubei/data/starRole.txt").getLines().toList
//    val tvList = Source.fromFile("src/main/scala/com/lomark/hubei/data/tvName.txt").getLines().toList

    // load data from driver client
//    val role = Source.fromFile("/home/hive/bj/ch/starRole.txt").getLines().toList
//    val tv = Source.fromFile("/home/hive/bj/ch/tvName.txt").getLines().toList

    // load data from jar
    val role = this.getClass().getResourceAsStream("/starRole.txt")
    val tv = this.getClass().getResourceAsStream("/tvName.txt")
    val starList = loadDataFromJar(role)
    val tvList = loadDataFromJar(tv)



    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)
    sqlc.setConf("spark.sql.parquet.compression.codec", "snappy")

    // 自定义测试数据
    //    val data = sc.makeRDD(List(("zs 2016",10),("zs2 20162",12),("zxxs 20167",110)))
    //    data.foreachPartition(fun)
    //sys.exit(0)

    val bcDataStar = sc.broadcast(starList)
    val bcDataTV = sc.broadcast(tvList)
    val data = sqlc.read.parquet(inputLogBase) /*.repartition(pars.toInt)*/
      .map({ a =>
      val log = LogsBean.buidFormParquet(a)
      //        var allKeys = ""
      //        var tuple = Tuple2(String, Int)
      var keyMap = mutable.HashMap[String, Int]()
      val starValue = bcDataStar.value
      val tvValue = bcDataTV.value
      val reqDate = log.requestdate.substring(0, 10)

      // 指定影视剧和人物的类型
      val starType = 0
      val tvType = 1

      /** 存在一天的数据中有少量前一天的日期，在这里过滤一下，如果跑多天的任务，这里的判断条件需注释掉 */
      if (inputLogBase.contains(reqDate.replace("-", ""))) {
        if (log.title.nonEmpty) {
          try {
            val regEx = "[`~!@#$%^&*()+=|{}':;',\\[\\].<>/?~！@#￥%……&*（）——+|{}【】‘；：”“’。，、？]"
            val p = Pattern.compile(regEx)
            val m = p.matcher(log.title)
            val titles = m.replaceAll("").replace("\t", "").trim.split(" +", log.title.length) // 少量空格，先替换掉
            for (str <- titles) {
              if (starValue.contains(str)) {
                //                if (allKeys.length > 0) {
                //                  allKeys = allKeys.concat(",")
                //                }
                //                allKeys = allKeys.concat(str).concat(" " + starType).concat(" " + reqDate) // 2017-02-12
                keyMap += (str.concat(" " + starType).concat(" " + reqDate) -> 1)
              }
              if (tvValue.contains(str)) {
                //                if (allKeys.length > 0) {
                //                  allKeys = allKeys.concat(",")
                //                }
                //                allKeys = allKeys.concat(str).concat(" " + tvType).concat(" " + reqDate) // 2017-02-12
                keyMap += (str.concat(" " + tvType).concat(" " + reqDate) -> 1)
              }
            }
          } catch {
            case e: Exception =>
              println(e)
          }
        }
        if (log.keywords.nonEmpty) {
          val keyWords = log.keywords.trim.split("\\|", log.keywords.length)
          for (key <- keyWords) {
            if (starValue.contains(key)) {
              //              if (allKeys.length > 0) {
              //                allKeys = allKeys.concat(",")
              //              }
              //              allKeys = allKeys.concat(key).concat(" " + starType).concat(" " + reqDate)
              keyMap += (key.concat(" " + starType).concat(" " + reqDate) -> 1)

            }
            if (tvValue.contains(key)) {
              //              if (allKeys.length > 0) {
              //                allKeys = allKeys.concat(",")
              //              }
              //              allKeys = allKeys.concat(key).concat(" " + tvType).concat(" " + reqDate)
              keyMap += (key.concat(" " + tvType).concat(" " + reqDate) -> 1)
            }
          }
        }
      }
      //        allKeys
      keyMap
    }) /*.filter(_ != "")*/

      // 新测试 20170224 未验证
      .flatMap(x => x).reduceByKey(_ + _)

      //        .map{x=> x.split(",").map((_,1))}.flatMap(x=>x).map(x=>x).reduceByKey(_ + _)
      //      .flatMap(_.split(",")).map((_, 1)).reduceByKey(_ + _)

      // 不直接写入mysql，就直接存文件，再导入
      .map {
      item =>
        val tmp = item._1.split(" ")
        val name = tmp(0)
        val typeName = tmp(1).toInt
        val date = tmp(2)
        val counts = item._2
        name + "," + typeName + "," + date + "," + counts
    }.coalesce(1).saveAsTextFile(outputPath)


    // 写mysql方法一：原始
    //              .foreachPartition(fun)
    //          .foreach(println)


    // 创建测试数据
    //    val dataRDD = sc.makeRDD(List(("zs 0 2016", 10), ("zs2 1 20162", 12), ("zxxs 0 20167", 110)))
    //
    //       // 读文件
    //          val dataRDD = data
    //      .map(item => {
    //      val tmp = item._1.split(" ")
    //      val name = tmp(0)
    //      val typeName = tmp(1).toInt
    //      val date = tmp(2)
    //      val counts = item._2
    //      val id = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()) + name.hashCode
    //      Row.apply(id, name, typeName, date, counts)
    //    })
    //
    //    val url = "jdbc:mysql://127.0.0.1:3306/test?useUnicode=true&characterEncoding=utf8&user=root&password=123"
    //    val schema = StructType(
    //      StructField("id", StringType) ::
    //        StructField("name", StringType) ::
    //        StructField("type", IntegerType) ::
    //        StructField("date", StringType) ::
    //        StructField("counts", IntegerType) :: Nil)
    //    val df = sqlc.createDataFrame(dataRDD, schema)


    // 写mysql方法2 这种api已经被舍弃了
    //    import sqlc.implicits._
    // 如何解决主键自增问题？？？
    //    df.createJDBCTable(url, "ranking5", true)
    //    df.insertIntoJDBC(url,"ranking6",true)
    //    sys.exit(0)
    //    df.registerTempTable("people")
    //    sqlc.sql("select name,date from people").collect.foreach(println)

    // 写mysql方法三
    val connectionProperties: Properties = new Properties()
    connectionProperties.setProperty("user", "root")
    connectionProperties.setProperty("password", "123")
    //    df.select(df("id"),df("name"), df("date"),df("counts")).write.mode(SaveMode.Append).jdbc(url, "tags_ranking", connectionProperties)

    // 从 mysql中读数据
    //    val jdbcDF = sqlc.read.jdbc(url,"ranking2",connectionProperties)
    //    val rowdata:util.List[Row]  = jdbcDF.collectAsList()
    //    jdbcDF.collect.foreach(println)


    // 读数据  报错了
    /* sqlc.read.format("jdbc")
       .option("url", "jdbc:mysql://127.0.0.1:3306/test?useUnicode=true&characterEncoding=utf8")
       .option("driver", "com.mysql.jdbc.Driver")
       .option("dbtable", "ranking2")
       .option("user", "root").option("password", "123")
       .load().show*/


    sc.stop()
  }

  /** load data from jar */
  def loadDataFromJar(path: InputStream) = {
    var list: List[String] = List()
    val br: BufferedReader = new BufferedReader(new InputStreamReader(path))
    var line: String = null
    do {
      line = br.readLine()
      if (line ne null) {
        //        println("============line===", line, list.size)
        list = line :: list
      }
    } while (line ne null)
    list
  }

  /** load data from hdfs */
  def loadDataFromHDFS(path: String): List[String] = {
    var bundleIn: FSDataInputStream = null
    var list: List[String] = List()
    var str: String = null
    try {
      val conf = new Configuration
      bundleIn = FileSystem.get(URI.create(path), conf).open(new Path(path))
      val star = new BufferedReader(new InputStreamReader(bundleIn))
      do {
        str = star.readLine()
        if (str ne null) {
          list = str :: list
        }
      } while (str ne null)
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        return list
    } finally {
      if (bundleIn != null) {
        IOUtils.closeStream(bundleIn)
      }
    }
    list
  }
}


