package com.ch.spark.ml.ctr

import org.apache.spark.sql.Row

/**
  * 原始日志实体类 (字段含义)
  */
class CTRBean(var sessionid: String, //会话标识
              val advertisersid: Int, //广告主id
              val adorderid: Int, //广告id
              val adcreativeid: Int, //广告创意id
              val adplatformproviderid: Int, //广告平台商id
              var requestmode: Int, //数据请求方式（1:请求、2:展示、3:点击）
              val adprice: Double, //广告价格
              val adppprice: Double, //平台商价格
              val client: Int, //设备类型 （1：android 2：ios 3：wp）
              val ispid: Int, //运营商id
              val networkmannerid: Int, //联网方式id
              val adspacetype: Int, //广告位类型（1：banner 2：插屏 3：全屏）
              val devicetype: Int, //设备类型（1：手机 2：平板）
              val apptype: Int, //应用类型id
              val iseffective: Int, //有效标识（有效指可以正常计费的）(0：无效 1：有效)
              val province: String, //rtb 省
              val city: String, //rtb 市
              val title: String, //标题
              val keywords: String //关键字

             ) extends Product with Serializable {

  //private val splitStr = "\t"

  override def productElement(n: Int): Any = n match {
    case 0 => sessionid
    case 1 => advertisersid
    case 2 => adorderid
    case 3 => adcreativeid
    case 4 => adplatformproviderid
    case 5 => requestmode
    case 6 => adprice
    case 7 => adppprice
    case 8 => client
    case 9 => ispid
    case 10 => networkmannerid
    case 11 => adspacetype
    case 12 => devicetype
    case 13 => apptype
    case 14 => iseffective
    case 15 => province
    case 16 => city
    case 17 => title
    case 18 => keywords
  }

  override def productArity: Int = 88

  override def canEqual(that: Any): Boolean = that.isInstanceOf[CTRBean]

  val splitStr = ","

  override def toString: String = {

    // requesetmode
    this.sessionid + splitStr + this.advertisersid + splitStr + this.adorderid + splitStr + this.adcreativeid + splitStr +
      this.adplatformproviderid + splitStr + this.adprice + splitStr + this.adppprice + splitStr + this.client + splitStr +
      this.ispid + splitStr + this.networkmannerid + splitStr +
      this.adspacetype + splitStr + this.devicetype + splitStr +
      this.apptype + splitStr +
      this.province + splitStr + this.city + splitStr +
      // 需要 one-hot Encoding的4个字段放一块，放最后
      (if (this.title == "") "0" else this.title) + splitStr + this.keywords


  }


}

object CTRBean {


  def buidFormParquet(line: Row): CTRBean = {

    new CTRBean(
      line.getAs[String]("sessionid"), line.getAs[Int]("advertisersid"), line.getAs[Int]("adorderid"),
      line.getAs[Int]("adcreativeid"), line.getAs[Int]("adplatformproviderid"),
      line.getAs[Int]("requestmode"), line.getAs[Double]("adprice"),
      line.getAs[Double]("adppprice"), line.getAs[Int]("client"),
      line.getAs[Int]("ispid"),
      line.getAs[Int]("networkmannerid"), line.getAs[Int]("adspacetype"),
      line.getAs[Int]("devicetype"), line.getAs[Int]("apptype"),line.getAs[Int]("iseffective"),
      line.getAs[String]("rtbprovince"), line.getAs[String]("rtbcity"),
      line.getAs[String]("title"), line.getAs[String]("keywords")
    )
  }

}

