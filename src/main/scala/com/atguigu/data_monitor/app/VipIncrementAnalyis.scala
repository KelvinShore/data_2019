package com.atguigu.data_monitor.app

import java.sql.{Connection, Date, DriverManager}
import java.text.SimpleDateFormat
import java.util.Properties

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import net.ipip.ipdb.{City, CityInfo}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.{ConnectionPool, DB, _}

import scala.collection.immutable

/**
  * Created by kelvin on 2019/7/12.
  */
object VipIncrementAnalyis {

  //提取出公共变量 转换算子公用
  val sdf=new SimpleDateFormat("yyyy-MM-dd")

  //从properties 文件里获取各种参数
  val prop=new Properties()
  prop.load(this.getClass.getClassLoader().getResourceAsStream("F:\\data_monitor\\src\\main\\resources\\VipIncrementAnalysis.properties"))

  //使用静态ip资源库
  val ipdb=new City(this.getClass().getClassLoader().getResource("ipipfree.ipdb").getPath())

  //获取jdbc相关参数
  val driver = prop.getProperty("jdbcDriver")
  val jdcbUrl = prop.getProperty("jdbcUrl")
  val jdbcUser = prop.getProperty("jdbcUser")
  val jdbcPassword = prop.getProperty("jdbcPassword")

  //设置jdbc
  Class.forName(driver)
  //设置连接池
  ConnectionPool.singleton(jdcbUrl,jdbcUser,jdbcPassword)

  def main(args: Array[String]): Unit = {

    //参数检测
    if(args.length != 1){
      println("Usage:Please input checkpointPath")
      System.exit(1)
    }
    //通过传入参数设置检查点
    val checkPoint: String = args(0)

    //通过getOrCreate 方式可以实现从Driver端失败恢复
    val ssc=StreamingContext.getOrCreate(checkPoint,()=>{
      getVipIncrementByCountry(checkPoint)
    })

    //启动流计算
    ssc.start()
    ssc.awaitTermination()


  }

  //通过地区统计vip新增数量
  def getVipIncrementByCountry(checkPoint:String):StreamingContext={
    //定义update函数
    val updateFunc: (Seq[Int], Option[Int]) => Some[Int] = (values: Seq[Int], state: Option[Int]) => {
      //本批次value求和
      val currentCount: Int = values.sum
      val previousCount: Int = state.getOrElse(0)
      Some(currentCount + previousCount)
    }
    //设置批处理间隔
    val processingInterval: Long = prop.getProperty("processingInterval").toLong

    //获取kafka相关参数
    val brokers: String = prop.getProperty("brokers")

    val sparkConf: SparkConf = new SparkConf().set("spark.streaming.stopGracefullyOnShutdown", "true")
      .setAppName(this.getClass.getSimpleName)

    val ssc = new StreamingContext(sparkConf,Seconds(processingInterval))

    val kafkaParams=Map[String,String]("metadata.broker.list" -> brokers,"enable.auto.commit"->"false")

    //获取offset
    val fromOffsets = DB.readOnly { implicit session => sql"select topic, part_id, offset from topic_offset".
      map { r =>
        TopicAndPartition(r.string(1), r.int(2)) -> r.long(3)
      }.list.apply().toMap
    }

    val messageHandler=(mmd:MessageAndMetadata[String,String])=>(mmd.topic+"-"+mmd.partition,mmd.message())
    var offsetRanges:Array[OffsetRange]=Array.empty[OffsetRange]

    //获取Dstream
    val message=KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder,(String,String)](ssc,kafkaParams,fromOffsets,messageHandler)

    //业务计算
    message.transform{rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.filter{msg =>
      //过滤非完成订单的数据,且验证数据合法性
      filterCompleteOrderData(msg)
    }.map{ msg=>
      //数据转换,返回((2019-04-03,北京),1)格式的数据
      getCountryAndDate(msg)

    }.updateStateByKey[Int]{
      //更新状态变量
      updateFunc
    }.filter{state =>
      //只保留最近2天的数据,这样返回给Driver的数据会非常小
      filter2DaysBeforeState(state)
    }.foreachRDD(rdd =>{
      //将最近两天的数据返回给Driver端
      val resultTuple: Array[((String, String), Int)] = rdd.collect()
      //开启事务
      DB.localTx{implicit session =>resultTuple.foreach(msg =>{
        val dt=msg._1._1
        val province=msg._1._2
        val cnt=msg._2.toLong
        //统计结果持久化到Mysql中
        sql"""replace into vip_increment_analysis(province,cnt,dt)values(${province},${cnt},${dt})""".executeUpdate().apply()
        println(msg)
      })
      for(o <- offsetRanges){
        println(o.topic,o.partition,o.fromOffset,o.untilOffset)
        //保存offset
        sql"""update topic_offset set offset = ${o.untilOffset} where topic=${o.topic} and part_id=${o.partition} """.update.apply()
      }

      }

    })
    //开启检查点
    ssc.checkpoint(checkPoint)
    message.checkpoint(Seconds(processingInterval*10))

    ssc
  }

  //只保留最近2天的状态,因为怕系统时间或数据采集过程中有稍许延迟,所以没有设计保留1天
  def filter2DaysBeforeState(state:((String,String),Int)):Boolean={
    //获取状态值对应的日期,并转换为13位的长整型时间戳
    val day: String = state._1._1
    val eventTime=sdf.parse(day).getTime
    //获取当前系统时间戳
    val currentTime: Long = System.currentTimeMillis()
    //两者比较,保留两天内的
    if(currentTime - eventTime >= 172800000){
      //false
      true
    }else{
      true
    }
  }

  /**
    * 过滤非完成订单的数据,且验证数据合法性
    * @param msg
    * @return
    */
  def filterCompleteOrderData(msg: (String, String)):Boolean={
    val fields: Array[String] = msg._2.split("\t")
    //切分后长度不为17 代表数据不合法
    if(fields.length==17){
      val eventType: String = msg._2.split("\t")(15)
      "completeOrder".equals(eventType)
    }else{
      false
    }


  }

  /**
    * 数据转换  返回((2019-04-03,北京),1)格式的数据
    * @param msg
    * @return
    */
  def getCountryAndDate(msg: (String, String)):((String,String),Int)={
    val fields: Array[String] = msg._2.split("\t")
    //获取ip地址
    val ip: String = fields(8)
    //获取事件时间
    val eventTime: Long = fields(16).toLong

    //根据日志中的eventTime获取对应的日期
    val date = new Date(eventTime * 1000)
    val eventDay: String = sdf.format(date)

    //根据ip获取省份信息
    var regionName = "未知"
    val info: CityInfo = ipdb.findInfo(ip,"CN")
    if(info != null){
      regionName=info.getRegionName()
    }
    ((eventDay,regionName),1)
  }

}
