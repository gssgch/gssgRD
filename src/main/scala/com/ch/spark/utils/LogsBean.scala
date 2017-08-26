package main.scala.com.ch.spark.utils

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

/**
  * 原始日志实体类 (字段含义)
  */
class LogsBean(val sessionid: String, //会话标识
               val advertisersid: Int, //广告主id
               val adorderid: Int, //广告id
               val adcreativeid: Int, //广告创意id
               val adplatformproviderid: Int, //广告平台商id
               val sdkversionnumber: String, //sdk版本号
               val adplatformkey: String, //平台商key
               val putinmodeltype: Int, //针对广告主的投放模式,1：展示量投放 2：点击量投放
               val requestmode: Int, //数据请求方式（1:请求、2:展示、3:点击）
               val adprice: Double, //广告价格
               val adppprice: Double, //平台商价格
               val requestdate: String, //请求时间,格式为：yyyy／m/dd hh:mm:ss
               val ip: String, //设备用户的真实ip地址
               val appid: String, //应用id
               val appname: String, //应用名称
               val uuid: String, //设备唯一标识，比如imei或者androidid等
               val device: String, //设备型号，如htc、iphone
               val client: Int, //设备类型 （1：android 2：ios 3：wp）
               val osversion: String, //设备操作系统版本，如4.0
               val density: String, //备屏幕的密度 android的取值为0.75、1、1.5,ios的取值为：1、2
               val pw: Int, //设备屏幕宽度
               val ph: Int, //设备屏幕高度
               val long: String, //设备所在经度
               val lat: String, //设备所在纬度
               val provincename: String, //设备所在省份名称
               val cityname: String, //设备所在城市名称
               val ispid: Int, //运营商id
               val ispname: String, //运营商名称
               val networkmannerid: Int, //联网方式id
               val networkmannername: String, //联网方式名称
               val iseffective: Int, //有效标识（有效指可以正常计费的）(0：无效 1：有效)
               val isbilling: Int, //是否收费（0：未收费 1：已收费）
               val adspacetype: Int, //广告位类型（1：banner 2：插屏 3：全屏）
               val adspacetypename: String, //广告位类型名称（banner、插屏、全屏）
               val devicetype: Int, //设备类型（1：手机 2：平板）
               val processnode: Int, //流程节点（1：请求量kpi 2：有效请求 3：广告请求）
               val apptype: Int, //应用类型id
               val district: String, //设备所在县名称
               val paymode: Int, //针对平台商的支付模式，1：展示量投放 2：点击量投放
               val isbid: Int, //是否rtb
               val bidprice: Double, //真实出价
               val winprice: Double, //RTB竞价成功价格（成本）
               val iswin: Int, //是否竞价成功
               val cur: String, //values:usd|rmb等
               val rate: Double, //汇率
               val cnywinprice: Double, //rtb竞价成功转换成人民币的价格
               val imei: String, //imei
               val mac: String, //mac
               val idfa: String, //idfa
               val openudid: String, //openudid
               val androidid: String, //androidid
               val rtbprovince: String, //rtb 省
               val rtbcity: String, //rtb 市
               val rtbdistrict: String, //rtb 区
               val rtbstreet: String, //rtb 街道
               val storeurl: String, //app的市场下载地址
               val realip: String, //真实ip
               val isqualityapp: Int, //优选标识
               val bidfloor: Double, //底价
               val aw: Int, //广告位的宽
               val ah: Int, //广告位的高
               val imeimd5: String, //imei_md5
               val macmd5: String, //mac_md5
               val idfamd5: String, //idfa_md5
               val openudidmd5: String, //openudid_md5
               val androididmd5: String, //androidid_md5
               val imeisha1: String, //imei_sha1
               val macsha1: String, //mac_sha1
               val idfasha1: String, //idfa_sha1
               val openudidsha1: String, //openudid_sha1
               val androididsha1: String, //androidid_sha1
               val uuidunknow: String, //uuid_unknow tanx密文
               val decuuidunknow: String, // 解密的tanx 明文
               val userid: String, //平台用户id
               val reqdate: String, //日期
               val reqhour: String, //小时
               val iptype: Int, //表示ip库类型，1为点媒ip库，2为广告协会的ip地理信息标准库，默认为1
               val initbidprice: Double, //初始出价
               val adpayment: Double, //转换后的广告消费(保留小数点后六位)
               val agentrate: Double, //代理商利润率
               val lomarkrate: Double, //点媒代理利润率
               val adxrate: Double, //媒介利润率
               val title: String, //标题
               val keywords: String, //关键字
               val tagid: String, //广告位标识
               val callbackdate: String, //回调时间 格式为:YYYY/mm/dd hh:mm:ss
               val channelid: String, //频道ID
               val mediatype: Int, //媒体类型:1长尾媒体 2视频媒体 3独立媒体
               val subchannelid: String, //二级频道ID
               val videouserid: String, //视频上传者id，上传视频的用户ID
               val sid: String, //节目id
               val vid: String  //视频id
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
    case 11 => requestdate
    case 12 => ip
    case 13 => appid
    case 14 => appname
    case 15 => uuid
    case 16 => device
    case 17 => client
    case 18 => osversion
    case 19 => density
    case 20 => pw
    case 21 => ph
    case 22 => long
    case 23 => lat
    case 24 => provincename
    case 25 => cityname
    case 26 => ispid
    case 27 => ispname
    case 28 => networkmannerid
    case 29 => networkmannername
    case 30 => iseffective
    case 31 => isbilling
    case 32 => adspacetype
    case 33 => adspacetypename
    case 34 => devicetype
    case 35 => processnode
    case 36 => apptype
    case 37 => district
    case 38 => paymode
    case 39 => isbid
    case 40 => bidprice
    case 41 => winprice
    case 42 => iswin
    case 43 => cur
    case 44 => rate
    case 45 => cnywinprice
    case 46 => imei
    case 47 => mac
    case 48 => idfa
    case 49 => openudid
    case 50 => androidid
    case 51 => rtbprovince
    case 52 => rtbcity
    case 53 => rtbdistrict
    case 54 => rtbstreet
    case 55 => storeurl
    case 56 => realip
    case 57 => isqualityapp
    case 58 => bidfloor
    case 59 => aw
    case 60 => ah
    case 61 => imeimd5
    case 62 => macmd5
    case 63 => idfamd5
    case 64 => openudidmd5
    case 65 => androididmd5
    case 66 => imeisha1
    case 67 => macsha1
    case 68 => idfasha1
    case 69 => openudidsha1
    case 70 => androididsha1
    case 71 => uuidunknow
    case 72 => decuuidunknow
    case 73 => userid
    case 74 => reqdate
    case 75 => reqhour
    case 76 => iptype
    case 77 => initbidprice
    case 78 => adpayment
    case 79 => agentrate
    case 80 => lomarkrate
    case 81 => adxrate
    case 82 => title
    case 83 => keywords
    case 84 => tagid
    case 85 => callbackdate
    case 86 => channelid
    case 87 => mediatype
    case 88 => subchannelid
    case 89 => videouserid
    case 90 => sid
    case 91 => vid

  }

  override def productArity: Int = 88

  override def canEqual(that: Any): Boolean = that.isInstanceOf[LogsBean]

  /* override def toString: String = {
     this.sessionid + splitStr + this.advertisersid + splitStr + this.adorderid + splitStr + this.adcreativeid + splitStr + this.adplatformproviderid + splitStr + this.sdkversionnumber + splitStr + this.adplatformkey + splitStr + this.putinmodeltype + splitStr + this.requestmode + splitStr + this.adprice + splitStr + this.adppprice + splitStr + this.requestdate + splitStr + this.ip + splitStr + this.appid + splitStr + this.appname + splitStr + this.uuid + splitStr + this.device + splitStr + this.client + splitStr + this.osversion + splitStr + this.density + splitStr + this.pw + splitStr + this.ph + splitStr + this.long + splitStr + this.lat + splitStr + this.provincename + splitStr + this.cityname + splitStr + this.ispid + splitStr + this.ispname + splitStr + this.networkmannerid + splitStr + this.networkmannername + splitStr + this.iseffective + splitStr + this.isbilling + splitStr + this.adspacetype + splitStr + this.adspacetypename + splitStr + this.devicetype + splitStr + this.processnode + splitStr + this.apptype + splitStr + this.district + splitStr + this.paymode + splitStr + this.isbid + splitStr + this.bidprice + splitStr + this.winprice + splitStr + this.iswin + splitStr + this.cur + splitStr + this.rate + splitStr + this.cnywinprice + splitStr + this.imei + splitStr + this.mac + splitStr + this.idfa + splitStr + this.openudid + splitStr + this.androidid + splitStr + this.rtbprovince + splitStr + this.rtbcity + splitStr + this.rtbdistrict + splitStr + this.rtbstreet + splitStr + this.storeurl + splitStr + this.realip + splitStr + this.isqualityapp + splitStr + this.bidfloor + splitStr + this.aw + splitStr + this.ah + splitStr + this.imeimd5 + splitStr + this.macmd5 + splitStr + this.idfamd5 + splitStr + this.openudidmd5 + splitStr + this.androididmd5 + splitStr + this.imeisha1 + splitStr + this.macsha1 + splitStr + this.idfasha1 + splitStr + this.openudidsha1 + splitStr + this.androididsha1 + splitStr + this.uuidunknow + splitStr + this.decuuidunknow + splitStr + this.userid + splitStr + this.reqdate + splitStr + this.reqhour + splitStr + this.iptype + splitStr + this.initbidprice + splitStr + this.adpayment + splitStr + this.agentrate + splitStr + this.lomarkrate + splitStr + this.adxrate
   }*/


}

object LogsBean {

  def buidEmpth(): LogsBean = {
    new LogsBean("", 0, 0, 0, 0, "", "", 0, 0, 0.0, 0.0, "", "", "", "", "", "", 0, "",
      "", 0, 0, "", "", "", "", 0, "", 0, "", 0, 0, 0, "", 0, 0, 0, "", 0, 0,
      0.0, 0.0, 0, "", 0.0, 0.0, "", "", "", "", "", "", "", "", "", "", "", 0, 0.0, 0, 0,
      "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 0, 0.0, 0.0, 0.0, 0.0, 0.0, "", "", "", "", "",0,"","","",""
    )
  }

  def buidFormParquet(line: Row): LogsBean = {
    if (line.length == 74) {
      new LogsBean(
        line.getAs[String]("sessionid"), line.getAs[Int]("advertisersid"), line.getAs[Int]("adorderid"), line.getAs[Int]("adcreativeid"), line.getAs[Int]("adplatformproviderid"),
        line.getAs[String]("sdkversionnumber"), line.getAs[String]("adplatformkey"), line.getAs[Int]("putinmodeltype"), line.getAs[Int]("requestmode"), line.getAs[Double]("adprice"),
        line.getAs[Double]("adppprice"), line.getAs[String]("requestdate"), line.getAs[String]("ip"), line.getAs[String]("appid"), line.getAs[String]("appname"), line.getAs[String]("uuid"),
        line.getAs[String]("device"), line.getAs[Int]("client"), line.getAs[String]("osversion"), line.getAs[String]("density"), line.getAs[Int]("pw"), line.getAs[Int]("ph"),
        line.getAs[String]("long"), line.getAs[String]("lat"), line.getAs[String]("provincename"), line.getAs[String]("cityname"), line.getAs[Int]("ispid"), line.getAs[String]("ispname"),
        line.getAs[Int]("networkmannerid"), line.getAs[String]("networkmannername"), line.getAs[Int]("iseffective"), line.getAs[Int]("isbilling"), line.getAs[Int]("adspacetype"),
        line.getAs[String]("adspacetypename"), line.getAs[Int]("devicetype"), line.getAs[Int]("processnode"), line.getAs[Int]("apptype"), line.getAs[String]("district"),
        line.getAs[Int]("paymode"), line.getAs[Int]("isbid"), line.getAs[Double]("bidprice"), line.getAs[Double]("winprice"), line.getAs[Int]("iswin"), line.getAs[String]("cur"),
        line.getAs[Double]("rate"), line.getAs[Double]("cnywinprice"), line.getAs[String]("imei"), line.getAs[String]("mac"), line.getAs[String]("idfa"), line.getAs[String]("openudid"), line.getAs[String]("androidid"),
        line.getAs[String]("rtbprovince"), line.getAs[String]("rtbcity"), line.getAs[String]("rtbdistrict"), line.getAs[String]("rtbstreet"), line.getAs[String]("storeurl"),
        line.getAs[String]("realip"), line.getAs[Int]("isqualityapp"), line.getAs[Double]("bidfloor"), line.getAs[Int]("aw"), line.getAs[Int]("ah"), line.getAs[String]("imeimd5"),
        line.getAs[String]("macmd5"), line.getAs[String]("idfamd5"), line.getAs[String]("openudidmd5"), line.getAs[String]("androididmd5"), line.getAs[String]("imeisha1"), line.getAs[String]("macsha1"),
        line.getAs[String]("idfasha1"), line.getAs[String]("openudidsha1"), line.getAs[String]("androididsha1"), line.getAs[String]("uuidunknow"), line.getAs[String]("decuuidunknow"),
        line.getAs[String]("userid"), "", "", 0, 0.0, 0.0, 0.0, 0.0, 0.0, "", "", "", "", "",0,"","","",""
      )
    } else if (line.length == 75) {
      new LogsBean(
        line.getAs[String]("sessionid"), line.getAs[Int]("advertisersid"), line.getAs[Int]("adorderid"), line.getAs[Int]("adcreativeid"), line.getAs[Int]("adplatformproviderid"),
        line.getAs[String]("sdkversionnumber"), line.getAs[String]("adplatformkey"), line.getAs[Int]("putinmodeltype"), line.getAs[Int]("requestmode"), line.getAs[Double]("adprice"),
        line.getAs[Double]("adppprice"), line.getAs[String]("requestdate"), line.getAs[String]("ip"), line.getAs[String]("appid"), line.getAs[String]("appname"), line.getAs[String]("uuid"),
        line.getAs[String]("device"), line.getAs[Int]("client"), line.getAs[String]("osversion"), line.getAs[String]("density"), line.getAs[Int]("pw"), line.getAs[Int]("ph"),
        line.getAs[String]("long"), line.getAs[String]("lat"), line.getAs[String]("provincename"), line.getAs[String]("cityname"), line.getAs[Int]("ispid"), line.getAs[String]("ispname"),
        line.getAs[Int]("networkmannerid"), line.getAs[String]("networkmannername"), line.getAs[Int]("iseffective"), line.getAs[Int]("isbilling"), line.getAs[Int]("adspacetype"),
        line.getAs[String]("adspacetypename"), line.getAs[Int]("devicetype"), line.getAs[Int]("processnode"), line.getAs[Int]("apptype"), line.getAs[String]("district"),
        line.getAs[Int]("paymode"), line.getAs[Int]("isbid"), line.getAs[Double]("bidprice"), line.getAs[Double]("winprice"), line.getAs[Int]("iswin"), line.getAs[String]("cur"),
        line.getAs[Double]("rate"), line.getAs[Double]("cnywinprice"), line.getAs[String]("imei"), line.getAs[String]("mac"), line.getAs[String]("idfa"), line.getAs[String]("openudid"), line.getAs[String]("androidid"),
        line.getAs[String]("rtbprovince"), line.getAs[String]("rtbcity"), line.getAs[String]("rtbdistrict"), line.getAs[String]("rtbstreet"), line.getAs[String]("storeurl"),
        line.getAs[String]("realip"), line.getAs[Int]("isqualityapp"), line.getAs[Double]("bidfloor"), line.getAs[Int]("aw"), line.getAs[Int]("ah"), line.getAs[String]("imeimd5"),
        line.getAs[String]("macmd5"), line.getAs[String]("idfamd5"), line.getAs[String]("openudidmd5"), line.getAs[String]("androididmd5"), line.getAs[String]("imeisha1"), line.getAs[String]("macsha1"),
        line.getAs[String]("idfasha1"), line.getAs[String]("openudidsha1"), line.getAs[String]("androididsha1"), line.getAs[String]("uuidunknow"), line.getAs[String]("decuuidunknow"),
        line.getAs[String]("userid"), "", "", line.getAs[Int]("iptype"), 0.0, 0.0, 0.0, 0.0, 0.0, "", "", "", "", "",0,"","","",""
      )
    } else if (line.length == 80) {
      new LogsBean(
        line.getAs[String]("sessionid"), line.getAs[Int]("advertisersid"), line.getAs[Int]("adorderid"), line.getAs[Int]("adcreativeid"), line.getAs[Int]("adplatformproviderid"),
        line.getAs[String]("sdkversionnumber"), line.getAs[String]("adplatformkey"), line.getAs[Int]("putinmodeltype"), line.getAs[Int]("requestmode"), line.getAs[Double]("adprice"),
        line.getAs[Double]("adppprice"), line.getAs[String]("requestdate"), line.getAs[String]("ip"), line.getAs[String]("appid"), line.getAs[String]("appname"), line.getAs[String]("uuid"),
        line.getAs[String]("device"), line.getAs[Int]("client"), line.getAs[String]("osversion"), line.getAs[String]("density"), line.getAs[Int]("pw"), line.getAs[Int]("ph"),
        line.getAs[String]("long"), line.getAs[String]("lat"), line.getAs[String]("provincename"), line.getAs[String]("cityname"), line.getAs[Int]("ispid"), line.getAs[String]("ispname"),
        line.getAs[Int]("networkmannerid"), line.getAs[String]("networkmannername"), line.getAs[Int]("iseffective"), line.getAs[Int]("isbilling"), line.getAs[Int]("adspacetype"),
        line.getAs[String]("adspacetypename"), line.getAs[Int]("devicetype"), line.getAs[Int]("processnode"), line.getAs[Int]("apptype"), line.getAs[String]("district"),
        line.getAs[Int]("paymode"), line.getAs[Int]("isbid"), line.getAs[Double]("bidprice"), line.getAs[Double]("winprice"), line.getAs[Int]("iswin"), line.getAs[String]("cur"),
        line.getAs[Double]("rate"), line.getAs[Double]("cnywinprice"), line.getAs[String]("imei"), line.getAs[String]("mac"), line.getAs[String]("idfa"), line.getAs[String]("openudid"), line.getAs[String]("androidid"),
        line.getAs[String]("rtbprovince"), line.getAs[String]("rtbcity"), line.getAs[String]("rtbdistrict"), line.getAs[String]("rtbstreet"), line.getAs[String]("storeurl"),
        line.getAs[String]("realip"), line.getAs[Int]("isqualityapp"), line.getAs[Double]("bidfloor"), line.getAs[Int]("aw"), line.getAs[Int]("ah"), line.getAs[String]("imeimd5"),
        line.getAs[String]("macmd5"), line.getAs[String]("idfamd5"), line.getAs[String]("openudidmd5"), line.getAs[String]("androididmd5"), line.getAs[String]("imeisha1"), line.getAs[String]("macsha1"),
        line.getAs[String]("idfasha1"), line.getAs[String]("openudidsha1"), line.getAs[String]("androididsha1"), line.getAs[String]("uuidunknow"), line.getAs[String]("decuuidunknow"),
        line.getAs[String]("userid"), "", "", line.getAs[Int]("iptype"), line.getAs[Double]("initbidprice"),
        line.getAs[Double]("adpayment"), line.getAs[Double]("agentrate"), line.getAs[Double]("lomarkrate"), line.getAs[Double]("adxrate"), "", "", "", "", "",0,"","","",""
      )
    } else if (line.length == 83) {
      new LogsBean(
        line.getAs[String]("sessionid"), line.getAs[Int]("advertisersid"), line.getAs[Int]("adorderid"), line.getAs[Int]("adcreativeid"), line.getAs[Int]("adplatformproviderid"),
        line.getAs[String]("sdkversionnumber"), line.getAs[String]("adplatformkey"), line.getAs[Int]("putinmodeltype"), line.getAs[Int]("requestmode"), line.getAs[Double]("adprice"),
        line.getAs[Double]("adppprice"), line.getAs[String]("requestdate"), line.getAs[String]("ip"), line.getAs[String]("appid"), line.getAs[String]("appname"), line.getAs[String]("uuid"),
        line.getAs[String]("device"), line.getAs[Int]("client"), line.getAs[String]("osversion"), line.getAs[String]("density"), line.getAs[Int]("pw"), line.getAs[Int]("ph"),
        line.getAs[String]("long"), line.getAs[String]("lat"), line.getAs[String]("provincename"), line.getAs[String]("cityname"), line.getAs[Int]("ispid"), line.getAs[String]("ispname"),
        line.getAs[Int]("networkmannerid"), line.getAs[String]("networkmannername"), line.getAs[Int]("iseffective"), line.getAs[Int]("isbilling"), line.getAs[Int]("adspacetype"),
        line.getAs[String]("adspacetypename"), line.getAs[Int]("devicetype"), line.getAs[Int]("processnode"), line.getAs[Int]("apptype"), line.getAs[String]("district"),
        line.getAs[Int]("paymode"), line.getAs[Int]("isbid"), line.getAs[Double]("bidprice"), line.getAs[Double]("winprice"), line.getAs[Int]("iswin"), line.getAs[String]("cur"),
        line.getAs[Double]("rate"), line.getAs[Double]("cnywinprice"), line.getAs[String]("imei"), line.getAs[String]("mac"), line.getAs[String]("idfa"), line.getAs[String]("openudid"), line.getAs[String]("androidid"),
        line.getAs[String]("rtbprovince"), line.getAs[String]("rtbcity"), line.getAs[String]("rtbdistrict"), line.getAs[String]("rtbstreet"), line.getAs[String]("storeurl"),
        line.getAs[String]("realip"), line.getAs[Int]("isqualityapp"), line.getAs[Double]("bidfloor"), line.getAs[Int]("aw"), line.getAs[Int]("ah"), line.getAs[String]("imeimd5"),
        line.getAs[String]("macmd5"), line.getAs[String]("idfamd5"), line.getAs[String]("openudidmd5"), line.getAs[String]("androididmd5"), line.getAs[String]("imeisha1"), line.getAs[String]("macsha1"),
        line.getAs[String]("idfasha1"), line.getAs[String]("openudidsha1"), line.getAs[String]("androididsha1"), line.getAs[String]("uuidunknow"), line.getAs[String]("decuuidunknow"),
        line.getAs[String]("userid"), "", "", line.getAs[Int]("iptype"), line.getAs[Double]("initbidprice"), line.getAs[Double]("adpayment"), line.getAs[Double]("agentrate"),
        line.getAs[Double]("lomarkrate"), line.getAs[Double]("adxrate"), line.getAs[String]("title"), line.getAs[String]("keywords"), line.getAs[String]("tagid"), "", "",0,"","","",""
      )
    } else if (line.length == 84) {
      new LogsBean(
        line.getAs[String]("sessionid"), line.getAs[Int]("advertisersid"), line.getAs[Int]("adorderid"), line.getAs[Int]("adcreativeid"), line.getAs[Int]("adplatformproviderid"),
        line.getAs[String]("sdkversionnumber"), line.getAs[String]("adplatformkey"), line.getAs[Int]("putinmodeltype"), line.getAs[Int]("requestmode"), line.getAs[Double]("adprice"),
        line.getAs[Double]("adppprice"), line.getAs[String]("requestdate"), line.getAs[String]("ip"), line.getAs[String]("appid"), line.getAs[String]("appname"), line.getAs[String]("uuid"),
        line.getAs[String]("device"), line.getAs[Int]("client"), line.getAs[String]("osversion"), line.getAs[String]("density"), line.getAs[Int]("pw"), line.getAs[Int]("ph"),
        line.getAs[String]("long"), line.getAs[String]("lat"), line.getAs[String]("provincename"), line.getAs[String]("cityname"), line.getAs[Int]("ispid"), line.getAs[String]("ispname"),
        line.getAs[Int]("networkmannerid"), line.getAs[String]("networkmannername"), line.getAs[Int]("iseffective"), line.getAs[Int]("isbilling"), line.getAs[Int]("adspacetype"),
        line.getAs[String]("adspacetypename"), line.getAs[Int]("devicetype"), line.getAs[Int]("processnode"), line.getAs[Int]("apptype"), line.getAs[String]("district"),
        line.getAs[Int]("paymode"), line.getAs[Int]("isbid"), line.getAs[Double]("bidprice"), line.getAs[Double]("winprice"), line.getAs[Int]("iswin"), line.getAs[String]("cur"),
        line.getAs[Double]("rate"), line.getAs[Double]("cnywinprice"), line.getAs[String]("imei"), line.getAs[String]("mac"), line.getAs[String]("idfa"), line.getAs[String]("openudid"), line.getAs[String]("androidid"),
        line.getAs[String]("rtbprovince"), line.getAs[String]("rtbcity"), line.getAs[String]("rtbdistrict"), line.getAs[String]("rtbstreet"), line.getAs[String]("storeurl"),
        line.getAs[String]("realip"), line.getAs[Int]("isqualityapp"), line.getAs[Double]("bidfloor"), line.getAs[Int]("aw"), line.getAs[Int]("ah"), line.getAs[String]("imeimd5"),
        line.getAs[String]("macmd5"), line.getAs[String]("idfamd5"), line.getAs[String]("openudidmd5"), line.getAs[String]("androididmd5"), line.getAs[String]("imeisha1"), line.getAs[String]("macsha1"),
        line.getAs[String]("idfasha1"), line.getAs[String]("openudidsha1"), line.getAs[String]("androididsha1"), line.getAs[String]("uuidunknow"), line.getAs[String]("decuuidunknow"),
        line.getAs[String]("userid"), "", "", line.getAs[Int]("iptype"), line.getAs[Double]("initbidprice"), line.getAs[Double]("adpayment"), line.getAs[Double]("agentrate"),
        line.getAs[Double]("lomarkrate"), line.getAs[Double]("adxrate"), line.getAs[String]("title"), line.getAs[String]("keywords"), line.getAs[String]("tagid"), line.getAs[String]("callbackdate"),
        "",0,"","","",""
      )
    } else if (line.length == 85){
      new LogsBean(
        line.getAs[String]("sessionid"), line.getAs[Int]("advertisersid"), line.getAs[Int]("adorderid"), line.getAs[Int]("adcreativeid"), line.getAs[Int]("adplatformproviderid"),
        line.getAs[String]("sdkversionnumber"), line.getAs[String]("adplatformkey"), line.getAs[Int]("putinmodeltype"), line.getAs[Int]("requestmode"), line.getAs[Double]("adprice"),
        line.getAs[Double]("adppprice"), line.getAs[String]("requestdate"), line.getAs[String]("ip"), line.getAs[String]("appid"), line.getAs[String]("appname"), line.getAs[String]("uuid"),
        line.getAs[String]("device"), line.getAs[Int]("client"), line.getAs[String]("osversion"), line.getAs[String]("density"), line.getAs[Int]("pw"), line.getAs[Int]("ph"),
        line.getAs[String]("long"), line.getAs[String]("lat"), line.getAs[String]("provincename"), line.getAs[String]("cityname"), line.getAs[Int]("ispid"), line.getAs[String]("ispname"),
        line.getAs[Int]("networkmannerid"), line.getAs[String]("networkmannername"), line.getAs[Int]("iseffective"), line.getAs[Int]("isbilling"), line.getAs[Int]("adspacetype"),
        line.getAs[String]("adspacetypename"), line.getAs[Int]("devicetype"), line.getAs[Int]("processnode"), line.getAs[Int]("apptype"), line.getAs[String]("district"),
        line.getAs[Int]("paymode"), line.getAs[Int]("isbid"), line.getAs[Double]("bidprice"), line.getAs[Double]("winprice"), line.getAs[Int]("iswin"), line.getAs[String]("cur"),
        line.getAs[Double]("rate"), line.getAs[Double]("cnywinprice"), line.getAs[String]("imei"), line.getAs[String]("mac"), line.getAs[String]("idfa"), line.getAs[String]("openudid"), line.getAs[String]("androidid"),
        line.getAs[String]("rtbprovince"), line.getAs[String]("rtbcity"), line.getAs[String]("rtbdistrict"), line.getAs[String]("rtbstreet"), line.getAs[String]("storeurl"),
        line.getAs[String]("realip"), line.getAs[Int]("isqualityapp"), line.getAs[Double]("bidfloor"), line.getAs[Int]("aw"), line.getAs[Int]("ah"), line.getAs[String]("imeimd5"),
        line.getAs[String]("macmd5"), line.getAs[String]("idfamd5"), line.getAs[String]("openudidmd5"), line.getAs[String]("androididmd5"), line.getAs[String]("imeisha1"), line.getAs[String]("macsha1"),
        line.getAs[String]("idfasha1"), line.getAs[String]("openudidsha1"), line.getAs[String]("androididsha1"), line.getAs[String]("uuidunknow"), line.getAs[String]("decuuidunknow"),
        line.getAs[String]("userid"), "", "", line.getAs[Int]("iptype"), line.getAs[Double]("initbidprice"), line.getAs[Double]("adpayment"), line.getAs[Double]("agentrate"),
        line.getAs[Double]("lomarkrate"), line.getAs[Double]("adxrate"), line.getAs[String]("title"), line.getAs[String]("keywords"), line.getAs[String]("tagid"), line.getAs[String]("callbackdate"),
        line.getAs[String]("channelid"),0,"","","",""
      )
    }else if (line.length == 86){
      new LogsBean(
        line.getAs[String]("sessionid"), line.getAs[Int]("advertisersid"), line.getAs[Int]("adorderid"), line.getAs[Int]("adcreativeid"), line.getAs[Int]("adplatformproviderid"),
        line.getAs[String]("sdkversionnumber"), line.getAs[String]("adplatformkey"), line.getAs[Int]("putinmodeltype"), line.getAs[Int]("requestmode"), line.getAs[Double]("adprice"),
        line.getAs[Double]("adppprice"), line.getAs[String]("requestdate"), line.getAs[String]("ip"), line.getAs[String]("appid"), line.getAs[String]("appname"), line.getAs[String]("uuid"),
        line.getAs[String]("device"), line.getAs[Int]("client"), line.getAs[String]("osversion"), line.getAs[String]("density"), line.getAs[Int]("pw"), line.getAs[Int]("ph"),
        line.getAs[String]("long"), line.getAs[String]("lat"), line.getAs[String]("provincename"), line.getAs[String]("cityname"), line.getAs[Int]("ispid"), line.getAs[String]("ispname"),
        line.getAs[Int]("networkmannerid"), line.getAs[String]("networkmannername"), line.getAs[Int]("iseffective"), line.getAs[Int]("isbilling"), line.getAs[Int]("adspacetype"),
        line.getAs[String]("adspacetypename"), line.getAs[Int]("devicetype"), line.getAs[Int]("processnode"), line.getAs[Int]("apptype"), line.getAs[String]("district"),
        line.getAs[Int]("paymode"), line.getAs[Int]("isbid"), line.getAs[Double]("bidprice"), line.getAs[Double]("winprice"), line.getAs[Int]("iswin"), line.getAs[String]("cur"),
        line.getAs[Double]("rate"), line.getAs[Double]("cnywinprice"), line.getAs[String]("imei"), line.getAs[String]("mac"), line.getAs[String]("idfa"), line.getAs[String]("openudid"), line.getAs[String]("androidid"),
        line.getAs[String]("rtbprovince"), line.getAs[String]("rtbcity"), line.getAs[String]("rtbdistrict"), line.getAs[String]("rtbstreet"), line.getAs[String]("storeurl"),
        line.getAs[String]("realip"), line.getAs[Int]("isqualityapp"), line.getAs[Double]("bidfloor"), line.getAs[Int]("aw"), line.getAs[Int]("ah"), line.getAs[String]("imeimd5"),
        line.getAs[String]("macmd5"), line.getAs[String]("idfamd5"), line.getAs[String]("openudidmd5"), line.getAs[String]("androididmd5"), line.getAs[String]("imeisha1"), line.getAs[String]("macsha1"),
        line.getAs[String]("idfasha1"), line.getAs[String]("openudidsha1"), line.getAs[String]("androididsha1"), line.getAs[String]("uuidunknow"), line.getAs[String]("decuuidunknow"),
        line.getAs[String]("userid"), "", "", line.getAs[Int]("iptype"), line.getAs[Double]("initbidprice"), line.getAs[Double]("adpayment"), line.getAs[Double]("agentrate"),
        line.getAs[Double]("lomarkrate"), line.getAs[Double]("adxrate"), line.getAs[String]("title"), line.getAs[String]("keywords"), line.getAs[String]("tagid"), line.getAs[String]("callbackdate"),
        line.getAs[String]("channelid"),line.getAs[Int]("mediatype"),"","","",""
      )
    }
//    else if (line.length == 90){
    else{
      new LogsBean(
        line.getAs[String]("sessionid"), line.getAs[Int]("advertisersid"), line.getAs[Int]("adorderid"), line.getAs[Int]("adcreativeid"), line.getAs[Int]("adplatformproviderid"),
        line.getAs[String]("sdkversionnumber"), line.getAs[String]("adplatformkey"), line.getAs[Int]("putinmodeltype"), line.getAs[Int]("requestmode"), line.getAs[Double]("adprice"),
        line.getAs[Double]("adppprice"), line.getAs[String]("requestdate"), line.getAs[String]("ip"), line.getAs[String]("appid"), line.getAs[String]("appname"), line.getAs[String]("uuid"),
        line.getAs[String]("device"), line.getAs[Int]("client"), line.getAs[String]("osversion"), line.getAs[String]("density"), line.getAs[Int]("pw"), line.getAs[Int]("ph"),
        line.getAs[String]("long"), line.getAs[String]("lat"), line.getAs[String]("provincename"), line.getAs[String]("cityname"), line.getAs[Int]("ispid"), line.getAs[String]("ispname"),
        line.getAs[Int]("networkmannerid"), line.getAs[String]("networkmannername"), line.getAs[Int]("iseffective"), line.getAs[Int]("isbilling"), line.getAs[Int]("adspacetype"),
        line.getAs[String]("adspacetypename"), line.getAs[Int]("devicetype"), line.getAs[Int]("processnode"), line.getAs[Int]("apptype"), line.getAs[String]("district"),
        line.getAs[Int]("paymode"), line.getAs[Int]("isbid"), line.getAs[Double]("bidprice"), line.getAs[Double]("winprice"), line.getAs[Int]("iswin"), line.getAs[String]("cur"),
        line.getAs[Double]("rate"), line.getAs[Double]("cnywinprice"), line.getAs[String]("imei"), line.getAs[String]("mac"), line.getAs[String]("idfa"), line.getAs[String]("openudid"), line.getAs[String]("androidid"),
        line.getAs[String]("rtbprovince"), line.getAs[String]("rtbcity"), line.getAs[String]("rtbdistrict"), line.getAs[String]("rtbstreet"), line.getAs[String]("storeurl"),
        line.getAs[String]("realip"), line.getAs[Int]("isqualityapp"), line.getAs[Double]("bidfloor"), line.getAs[Int]("aw"), line.getAs[Int]("ah"), line.getAs[String]("imeimd5"),
        line.getAs[String]("macmd5"), line.getAs[String]("idfamd5"), line.getAs[String]("openudidmd5"), line.getAs[String]("androididmd5"), line.getAs[String]("imeisha1"), line.getAs[String]("macsha1"),
        line.getAs[String]("idfasha1"), line.getAs[String]("openudidsha1"), line.getAs[String]("androididsha1"), line.getAs[String]("uuidunknow"), line.getAs[String]("decuuidunknow"),
        line.getAs[String]("userid"), "", "", line.getAs[Int]("iptype"), line.getAs[Double]("initbidprice"), line.getAs[Double]("adpayment"), line.getAs[Double]("agentrate"),
        line.getAs[Double]("lomarkrate"), line.getAs[Double]("adxrate"), line.getAs[String]("title"), line.getAs[String]("keywords"), line.getAs[String]("tagid"), line.getAs[String]("callbackdate"),
        line.getAs[String]("channelid"),line.getAs[Int]("mediatype"),line.getAs[String]("subchannelid"),line.getAs[String]("videouserid"),line.getAs[String]("sid"),line.getAs[String]("vid")
      )
    }
  }

  def line2Object(str: String): LogsBean = {
    if (StringUtils.isNotEmpty(str)) {
      var obj: Array[String] = str.split("\t", str.length)
      new LogsBean(obj(0), obj(1).toInt, obj(2).toInt, obj(3).toInt, obj(4).toInt, obj(5), obj(6), obj(7).toInt, obj(8).toInt, obj(9).toDouble,
        obj(10).toDouble, obj(11), obj(12), obj(13), obj(14), obj(15), obj(16), obj(17).toInt, obj(18), obj(19),
        obj(20).toInt, obj(21).toInt, obj(22), obj(23), obj(24), obj(25), obj(26).toInt, obj(27), obj(28).toInt, obj(29),
        obj(30).toInt, obj(31).toInt, obj(32).toInt, obj(33), obj(34).toInt, obj(35).toInt, obj(36).toInt, obj(37), obj(38).toInt, obj(39).toInt,
        obj(40).toDouble, obj(41).toDouble, obj(42).toInt, obj(43), obj(44).toDouble, obj(45).toDouble, obj(46), obj(47), obj(48), obj(49),
        obj(50), obj(51), obj(52), obj(53), obj(54), obj(55), obj(56), obj(57).toInt, obj(58).toDouble, obj(59).toInt,
        obj(60).toInt, obj(61), obj(62), obj(63), obj(64), obj(65), obj(66), obj(67), obj(68), obj(69),
        obj(70), obj(71), obj(72), obj(73), obj(74), obj(75), obj(76).toInt, obj(77).toDouble, obj(78).toDouble, obj(79).toDouble,
        obj(80).toDouble, obj(81).toDouble, obj(82), obj(83), obj(84), obj(85), obj(86),obj(87).toInt,obj(88),obj(89),obj(90),obj(91))
    } else buidEmpth()
  }
}

