package com.atguigu.gmall.realtime.app

import java.lang

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.gmall.realtime.bean.{PageActionLog, PageDisplayLog, PageLog, StartLog}
import com.atguigu.gmall.realtime.util.MyKafkaUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 日志数据的消费分流
 * 1.准备实时处理环境streamingContext
 * 2.从kafka中消费数据
 * 3.处理数据
 *  3.1转换数据结构
 * 专用结构 Bean
 * 通用结构 Map JsonObject
 *  3.2分流
 * 4.写出到DWD层
 */
object Ods_BaseLogApp {
  def main(args: Array[String]): Unit = {
    //准备实时环境
    //TODO 注意并行度与kafka中topic的分区个数的对应关系
    val conf: SparkConf = new SparkConf().setAppName("ods_base_log_app").setMaster("local[3]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))

    //2.从kafka中消费数据
    val topicName: String = "ODS_BASE_LOG_1018" //对应生成器配置中的主题名
    val groupId: String = "ODS_BASES_LOG_GROUP_1018"
    val KafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId)
    //3.处理数据
    //3.1转换数据结构
    val jsonObjDStream: DStream[JSONObject] = KafkaDStream.map(
      (consumerRecord: ConsumerRecord[String, String]) => {
        //获取ConsumerRecord中的value，value就是日志数据
        val log: String = consumerRecord.value()
        //转换成json对象
        val jsonObj: JSONObject = JSON.parseObject(log)
        //返回
        jsonObj
      }
    )
    //    jsonObjDStream.print()
    //3.2分流
    //日志数据：
    //页面访问数据：公共字段、页面数据、曝光数据、事件数据、错误数据
    //启动数据：启动数据、公共字段、错误数据

    val DWD_PAGE_LOG_TOPIC: String = "DWD_PAGE_LOG_TOPIC_1018" //页面访问
    val DWD_PAGE_DISPLAY_TOPIC: String = "DWD_PAGE_DISPLAY_TOPIC_1018" //页面曝光
    val DWD_PAGE_ACTION_TOPIC: String = "DWD_PAGE_ACTION_TOPIC_1018" //页面事件
    val DWD_START_LOG_TOPIC: String = "DWD_START_LOG_TOPIC_1018" //启动数据
    val DWD_ERROR_LOG_TOPIC: String = "DWD_ERROR_LOG_TOPIC_1018" //错误数据

    //分流的规则：
    //  错误数据：不做任何拆分，只要包含错误字段，直接整条数据发送到对应的topic
    //  页面数据：拆分成页面访问，曝光，事件 分别发送到对应的topic
    //  启动数据：发送到对应的topic

    jsonObjDStream.foreachRDD(
      rdd => {
        rdd.foreach(
          jsonObj => {
            //分流过程
            //分流错误数据
            val errObj: JSONObject = jsonObj.getJSONObject("err")
            if (errObj != null) {
              //将错误数据发送到错误主题,需要将json数据装换,json是阿里的是Java写的，需要传一个值new SerializeConfig(true),不再去找get，set方法（java对象需要一个get，set方法，Scala不用get，set方法）
              MyKafkaUtils.send(DWD_ERROR_LOG_TOPIC, JSON.toJSONString(jsonObj,new SerializeConfig(true)))
            } else {
              //提取公共字段
              val commonObj: JSONObject = jsonObj.getJSONObject("common")
              val ar: String = commonObj.getString("ar")
              val uid: String = commonObj.getString("uid")
              val os: String = commonObj.getString("os")
              val ch: String = commonObj.getString("ch")
              val is_new: String = commonObj.getString("is_new")
              val md: String = commonObj.getString("md")
              val mid: String = commonObj.getString("mid")
              val vc: String = commonObj.getString("vc")
              val ba: String = commonObj.getString("ba")
              //提取时间戳
              val ts: lang.Long = jsonObj.getLong("ts")
              //页面数据
              val pageObj: JSONObject = jsonObj.getJSONObject("page")
              if (pageObj != null) {
                //提取page字段
                val page_id: String = pageObj.getString("page_id")
                val pageItem: String = pageObj.getString("item")
                val pageType: String = pageObj.getString("item_type")
                val duringTime: lang.Long = pageObj.getLong("during_time")
                val last_page_id: String = pageObj.getString("last_page_id")
                val source_type: String = pageObj.getString("source_type")
                //封装成pagelog
                val pageLog: PageLog = PageLog(mid, uid, ar, ch, is_new, md, os, vc, ba, page_id, last_page_id, pageItem, pageType, duringTime, source_type, ts)
                //发送到页面访问主题,json是阿里的是Java写的，需要传一个值new SerializeConfig(true),不再去找get，set方法（java对象需要一个get，set方法，Scala不用get，set方法）
                MyKafkaUtils.send(DWD_PAGE_LOG_TOPIC, JSON.toJSONString(pageLog, new SerializeConfig(true)))
                //提取曝光数据,[{},{}],jsonaerray是java代码 需要自己写for
                val displaysJsonArr: JSONArray = jsonObj.getJSONArray("displays")
                if (displaysJsonArr != null && displaysJsonArr.size() > 0) {
                  //循环拿到每个曝光
                  for (i <- 0 until displaysJsonArr.size()) {
                    //提取曝光字段
                    val displayObj: JSONObject = displaysJsonArr.getJSONObject(i)
                    val displayType: String = displayObj.getString("display_type")
                    val displayItem: String = displayObj.getString("item")
                    val displayItemType: String = displayObj.getString("item_type")
                    val PosId: String = displayObj.getString("pos_id")
                    val order: String = displayObj.getString("order")
                    //封装对象
                    val pageDisplayLog: PageDisplayLog = PageDisplayLog(md, uid, ar, ch, is_new, md, os, vc, ba, page_id, last_page_id, pageItem, pageType, duringTime, source_type, displayType, displayItem, displayItemType, order, PosId, ts)
                    MyKafkaUtils.send(DWD_PAGE_DISPLAY_TOPIC, JSON.toJSONString(pageDisplayLog, new SerializeConfig(true)))
                  }
                }
                //提取事件数据
                val actionsArr: JSONArray = jsonObj.getJSONArray("actions")
                if (actionsArr != null && actionsArr.size() > 0) {
                  for (i <- 0 until actionsArr.size()) {
                    val actionObj: JSONObject = actionsArr.getJSONObject(i)
                    val actionId: String = actionObj.getString("action_id")
                    val actionItem: String = actionObj.getString("item")
                    val actionItemType: String = actionObj.getString("item_type")
                    val actionLog: PageActionLog = PageActionLog(md, uid, ar, ch, is_new, md, os, vc, ba, page_id, last_page_id, pageItem, pageType, duringTime, actionId, actionItem, actionItemType, source_type, ts)
                    MyKafkaUtils.send(DWD_PAGE_ACTION_TOPIC, JSON.toJSONString(actionLog, new SerializeConfig(true)))
                  }
                }
              }
              //启动数据
              val startObj: JSONObject = jsonObj.getJSONObject("start")
              if (startObj != null) {
                val entry: String = startObj.getString("entry")
                val open_ad_skip_ms: lang.Long = startObj.getLong("open_ad_skip_ms")
                val open_ad_ms: lang.Long = startObj.getLong("open_ad_ms")
                val loading_time: lang.Long = startObj.getLong("loading_time")
                val open_ad_id: String = startObj.getString("open_ad_id")
                val startLog: StartLog = StartLog(md, uid, ar, ch, is_new, md, os, vc, ba, entry, open_ad_id, loading_time, open_ad_ms, open_ad_skip_ms, ts)
                MyKafkaUtils.send(DWD_START_LOG_TOPIC, JSON.toJSONString(startLog, new SerializeConfig(true)))
              }

            }
          }
        )
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}
