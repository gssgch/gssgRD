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
              val sdkversionnumber: String, //sdk版本号
              val adplatformkey: String, //平台商key
              val putinmodeltype: Int, //针对广告主的投放模式,1：展示量投放 2：点击量投放
              var requestmode: Int, //数据请求方式（1:请求、2:展示、3:点击）
              val adprice: Double, //广告价格
              val adppprice: Double, //平台商价格
              val client: Int, //设备类型 （1：android 2：ios 3：wp）
              val pw: Int, //设备屏幕宽度
              val ph: Int, //设备屏幕高度
              val ispid: Int, //运营商id
              val networkmannerid: Int, //联网方式id
              val iseffective: Int, //有效标识（有效指可以正常计费的）(0：无效 1：有效)
              val isbilling: Int, //是否收费（0：未收费 1：已收费）
              val adspacetype: Int, //广告位类型（1：banner 2：插屏 3：全屏）
              val devicetype: Int, //设备类型（1：手机 2：平板）
              val processnode: Int, //流程节点（1：请求量kpi 2：有效请求 3：广告请求）
              val apptype: Int, //应用类型id
              val paymode: Int, //针对平台商的支付模式，1：展示量投放 2：点击量投放
              val isbid: Int, //是否rtb
              val bidprice: Double, //真实出价
              val winprice: Double, //RTB竞价成功价格（成本）

              val aw: Int, //广告位的宽
val ah: Int, //广告位的高
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
    case 5 => sdkversionnumber
    case 6 => adplatformkey
    case 7 => putinmodeltype
    case 8 => requestmode
    case 9 => adprice
    case 10 => adppprice
    case 11 => client
    case 12 => pw
    case 13 => ph
    case 14 => ispid
    case 15 => networkmannerid
    case 16 => iseffective
    case 17 => isbilling
    case 18 => adspacetype
    case 19 => devicetype
    case 20 => processnode
    case 21 => apptype
    case 22 => paymode
    case 23 => isbid
    case 24 => bidprice
    case 25 => winprice
    case 26 => aw
    case 27 => ah
    case 28 => title
    case 29 => keywords


  }

  override def productArity: Int = 88

  override def canEqual(that: Any): Boolean = that.isInstanceOf[CTRBean]

  val splitStr = ","

  // 去除一些无用的特征  uuid requestmode iseffective requestmode
  override def toString: String = {
    /* this.sessionid + splitStr + this.advertisersid + splitStr + this.adorderid + splitStr + this.adcreativeid + splitStr + this.adplatformproviderid + splitStr +
       this.sdkversionnumber + splitStr /*+ this.adplatformkey + splitStr*/ + this.putinmodeltype + splitStr +
 //      this.requestmode + splitStr +
       this.adprice + splitStr + this.adppprice + splitStr +
 //      this.requestdate + splitStr +
       this.ip + splitStr + this.appid + splitStr + this.appname + splitStr +
 //      this.uuid + splitStr +
       this.device + splitStr + this.client + splitStr + this.osversion + splitStr + this.density + splitStr + this.pw + splitStr +
       this.ph + splitStr + this.long + splitStr + this.lat + splitStr + this.provincename + splitStr + this.cityname + splitStr +
       this.ispid + splitStr + this.ispname + splitStr + this.networkmannerid + splitStr + this.networkmannername + splitStr +
 //      this.iseffective + splitStr +
       this.isbilling + splitStr + this.adspacetype + splitStr + this.adspacetypename + splitStr +
       this.devicetype + splitStr + this.processnode + splitStr + this.apptype + splitStr + this.district + splitStr +
       this.paymode + splitStr + this.isbid + splitStr + this.bidprice + splitStr + this.winprice + splitStr +
       this.aw + splitStr + this.ah + splitStr + this.title + splitStr + this.keywords*/


    // requesetmode 和 iseffective 去除
    this.sessionid + splitStr + this.advertisersid + splitStr + this.adorderid + splitStr + this.adcreativeid + splitStr +
      this.adplatformproviderid + splitStr +
      this.putinmodeltype + splitStr +this.adprice + splitStr + this.adppprice + splitStr +this.client + splitStr +
      this.pw + splitStr +this.ph + splitStr +this.ispid + splitStr + this.networkmannerid + splitStr+
      this.isbilling + splitStr + this.adspacetype + splitStr +this.devicetype + splitStr + this.processnode + splitStr +
      this.apptype + splitStr + this.paymode + splitStr + this.isbid + splitStr + this.bidprice + splitStr +
      this.winprice + splitStr +this.aw + splitStr + this.ah + splitStr +
      // 需要 one-hot Encoding的4个字段放一块，放最后
      this.sdkversionnumber + splitStr + this.adplatformkey + splitStr +this.title + splitStr + this.keywords


  }


}

object CTRBean {

  //  def buidEmpth(): CTRBean = {
  //    new CTRBean("", 0, 0, 0, 0, "", "", 0, 0, 0.0, 0.0, "", "", "", "", "", "", 0, "",
  //      "", 0, 0, "", "", "", "", 0, "", 0, "", 0, 0, 0, "", 0, 0, 0, "", 0, 0,
  //      0.0, 0.0, 0, "", 0.0, 0.0, "", "", "", "", "", "", "", "", "", "", "", 0, 0.0, 0, 0,
  //      "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 0, 0.0, 0.0, 0.0, 0.0, 0.0, "", "", "", "", "",0,"","","",""
  //    )
  //  }

  def buidFormParquet(line: Row): CTRBean = {
    //    if (line.length == 86){
    //      new CTRBean(
    //        line.getAs[String]("sessionid"), line.getAs[Int]("advertisersid"), line.getAs[Int]("adorderid"), line.getAs[Int]("adcreativeid"), line.getAs[Int]("adplatformproviderid"),
    //        line.getAs[String]("sdkversionnumber"), line.getAs[String]("adplatformkey"), line.getAs[Int]("putinmodeltype"), line.getAs[Int]("requestmode"), line.getAs[Double]("adprice"),
    //        line.getAs[Double]("adppprice"), line.getAs[String]("requestdate"), line.getAs[String]("ip"), line.getAs[String]("appid"), line.getAs[String]("appname"), line.getAs[String]("uuid"),
    //        line.getAs[String]("device"), line.getAs[Int]("client"), line.getAs[String]("osversion"), line.getAs[String]("density"), line.getAs[Int]("pw"), line.getAs[Int]("ph"),
    //        line.getAs[String]("long"), line.getAs[String]("lat"), line.getAs[String]("provincename"), line.getAs[String]("cityname"), line.getAs[Int]("ispid"), line.getAs[String]("ispname"),
    //        line.getAs[Int]("networkmannerid"), line.getAs[String]("networkmannername"), line.getAs[Int]("iseffective"), line.getAs[Int]("isbilling"), line.getAs[Int]("adspacetype"),
    //        line.getAs[String]("adspacetypename"), line.getAs[Int]("devicetype"), line.getAs[Int]("processnode"), line.getAs[Int]("apptype"), line.getAs[String]("district"),
    //        line.getAs[Int]("paymode"), line.getAs[Int]("isbid"), line.getAs[Double]("bidprice"), line.getAs[Double]("winprice"),
    //         line.getAs[Int]("aw"), line.getAs[Int]("ah"), "","","",""
    //      )
    //    }
    //    else if (line.length == 90){
    //    else{
    new CTRBean(
      line.getAs[String]("sessionid"), line.getAs[Int]("advertisersid"), line.getAs[Int]("adorderid"), line.getAs[Int]("adcreativeid"), line.getAs[Int]("adplatformproviderid"),
      line.getAs[String]("sdkversionnumber"), line.getAs[String]("adplatformkey"), line.getAs[Int]("putinmodeltype"), line.getAs[Int]("requestmode"), line.getAs[Double]("adprice"),
      line.getAs[Double]("adppprice"), line.getAs[Int]("client"), line.getAs[Int]("pw"), line.getAs[Int]("ph"),
      line.getAs[Int]("ispid"),
      line.getAs[Int]("networkmannerid"), line.getAs[Int]("iseffective"), line.getAs[Int]("isbilling"), line.getAs[Int]("adspacetype"),
      line.getAs[Int]("devicetype"), line.getAs[Int]("processnode"), line.getAs[Int]("apptype"),
      line.getAs[Int]("paymode"), line.getAs[Int]("isbid"), line.getAs[Double]("bidprice"), line.getAs[Double]("winprice"), line.getAs[Int]("aw"), line.getAs[Int]("ah"),
      line.getAs[String]("title"), line.getAs[String]("keywords")
    )
  }

  //  }

  //  def line2Object(str: String): CTRBean = {
  //    if (StringUtils.isNotEmpty(str)) {
  //      var obj: Array[String] = str.split("\t", str.length)
  //      new CTRBean(obj(0), obj(1).toInt, obj(2).toInt, obj(3).toInt, obj(4).toInt, obj(5), obj(6), obj(7).toInt, obj(8).toInt, obj(9).toDouble,
  //        obj(10).toDouble, obj(11), obj(12), obj(13), obj(14), obj(15), obj(16), obj(17).toInt, obj(18), obj(19),
  //        obj(20).toInt, obj(21).toInt, obj(22), obj(23), obj(24), obj(25), obj(26).toInt, obj(27), obj(28).toInt, obj(29),
  //        obj(30).toInt, obj(31).toInt, obj(32).toInt, obj(33), obj(34).toInt, obj(35).toInt, obj(36).toInt, obj(37), obj(38).toInt, obj(39).toInt,
  //        obj(40).toDouble, obj(41).toDouble, obj(42).toInt, obj(43), obj(44).toDouble, obj(45).toDouble, obj(46), obj(47), obj(48), obj(49),
  //        obj(50), obj(51), obj(52), obj(53), obj(54), obj(55), obj(56), obj(57).toInt, obj(58).toDouble, obj(59).toInt,
  //        obj(60).toInt, obj(61), obj(62), obj(63), obj(64), obj(65), obj(66), obj(67), obj(68), obj(69),
  //        obj(70), obj(71), obj(72), obj(73), obj(74), obj(75), obj(76).toInt, obj(77).toDouble, obj(78).toDouble, obj(79).toDouble,
  //        obj(80).toDouble, obj(81).toDouble, obj(82), obj(83), obj(84), obj(85), obj(86),obj(87).toInt,obj(88),obj(89),obj(90),obj(91))
  //    } else buidEmpth()
  //  }
}

