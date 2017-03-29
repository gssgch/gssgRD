

package main.scala.com.ch.spark.multiple

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.{SparkConf, SparkContext}


object MutiTextOutput {

  def main(args: Array[String]) {

    //input: from LomarkUsers  output
    //    if (args.length != 3) {
    //      println("Usage:MakeTags <inputLogBase> <outputPath> <partitions>")
    ////      System.exit(0)
    //    }
    //    val Array(inputLogBase, outputPath, pars) = args
    val inputLogBase = "src/main/scala/com/ch/spark/mutil/data/"
    val outputPath = "src/main/scala/com/ch/spark/mutil/data2"
    //
    val conf = new SparkConf().setAppName("MakeTags").setMaster("local[1]")
    //    //    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //    //      .registerKryoClasses(Array(classOf[UserFlagLog]))
    //
    val sc = new SparkContext(conf)
    //    val sqlc = new SQLContext(sc)
    //    sqlc.setConf("spark.sql.parquet.compression.codec", "snappy")

    val today = "N00010006:1,N4H010001:1,P01020001:1,UU05:1,N00030001:1,P01030001:1,UU10:1,N00040001:1,UU04100001:1,A17:1,P01010001:1,P01010002:4,APPWiFi万能压缩:1"


    val dcc_today = sc.makeRDD(today.split(",")).map(
      a => {
        val dc = a.split(":")
        (dc(0), dc(1).toDouble)
      })
    //      .saveAsTextFile("")

    dcc_today.
      saveAsHadoopFile("src/main/scala/com/ch/spark/mutil/d13/", classOf[Any], classOf[Any], classOf[MultiTextOutput])
    // save时指定的目录，然后再该目录下，会根据generateFileNameForKeyValue这个函数里的指定来生成文件名
  }
}

//
class MultiTextOutput extends MultipleTextOutputFormat[Any, Any] {
  //  override def generateActualKey(key: Any, value: Any): AnyRef = {
  //    val k = key.asInstanceOf[String]
  //    k.startsWith("P0101")
  //  }

  /**
    * 指定输出的文件名，和文件目录
    */
  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
    /* 1,源码中默认的设置 不会生成子目录，输出目录下都是文件，以编号为文件名，part-00000这种 */
    //    name

    /* 2.1,直接根据key生成文件名，key是啥，文件名就是啥 无子目录*/
    val k = key.asInstanceOf[String]
    //    k

    /* 2.2,直接根据value生成文件名，value是啥，文件名就是啥 无子目录 注意返回类型*/
    val v = value.asInstanceOf[Double]
    //    v.toString

    /*3.1,根据key和value，按照指定格式生成文件名,这里测试是根据value的次数，次数是输出目录的子目录，然后里面的文件名是k值*/
    //    v+"/"+k

    /*3.2,根据name和value，这里测试是根据value的次数，次数是子目录，然后里面的文件名是reduce的编号*/
    //    v+"/"+name

    /*3.3,根据name和key，这里测试是根据key值，次数是输出子目录，然后里面的文件名是reduce的编号*/
    //    k + "/" + name

    /*4,根据自定义条件来确定子目录名，这里模拟geoHash的识别和未识别的*/
    val pre = "defined"
    val unPre = "unDefined"
    if (k.startsWith("P0101") && (value.asInstanceOf[Double] > 2.0)) {
      //    if (v.contains("geoHash")) {
      unPre + "/" + name
    } else {
      pre + "/" + name
    }
  }

  /**
    * 指定输出的key  如不需要，可不重写
    */
    override def generateActualKey(key: Any, value: Any): AnyRef = {
      /*1,默认是key，不需要修改就不用重写该方法*/
  //    key

      /*2,不输出key，最终的文件里只有value的值*/
      //    NullWritable.get()

      /*3.1,对key进行判断，根据条件输出不同的key*/
  //    if (key.asInstanceOf[String].startsWith("P0101")) {
  //      key.asInstanceOf[String]
  //    } else {
  //      "xxx" // key为xxx
  //    }

      /*3.2,对value进行判断，根据条件输出不同的key*/
      if (value.asInstanceOf[Double]<2) {
        "test1"
      } else {
        "test2"
      }

      /*同样可以对key和value多条件判断*/
    }

  /** 指定输出的value */
  override def generateActualValue(key: Any, value: Any) = {
    /*1,默认是value，不需要修改就不用重写该方法*/
    //   value

    /*2,不输出value，最终的文件里只有key的值*/
    //  NullWritable.get()

    /*3,可以对key value进行单独和综合判断来指定value的输出*/
    // 应用场景：如果是未识别的geoHash，只需要取geohash的数据就可以了
    if (key.asInstanceOf[String].startsWith("P0101") && value.asInstanceOf[Double] < 2) {
      key.asInstanceOf[String].map(_.toString.split("01",-1)(0))
    } else {
      "test2"
    }


  }


}