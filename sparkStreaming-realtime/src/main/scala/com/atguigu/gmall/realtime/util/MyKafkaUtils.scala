package com.atguigu.gmall.realtime.util

import java.util

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.streaming.StreamingContext
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable

/**
 * kafka工具类，用于生产数据和消费数据
 */
object MyKafkaUtils {

  /**
   * 消费者配置
   * ConsumerConfig
   */
  private val consumerConfigs: mutable.Map[String, Object] = mutable.Map[String, Object](
    //kafka集群位置
//    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "node1:9092,node2:9092,node3:9092",
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> MyPropsUtils(MyConfig.KAFKA_BOOTSTRAP_SERVER),
    //kv反序列化器
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    //groupid
    //offset提交 自动 手动
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
    //offset自动提交间隔
    //    ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG
    //offset重置
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest"
  )


  /**
   * 基于sparkstreaming消费,获取到kafkaDStream,使用默认的offset
   */

  def getKafkaDStream(ssc: StreamingContext, topic: String, groupId: String): InputDStream[ConsumerRecord[String, String]] = {
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfigs))
    kafkaDStream
  }

  /**
   * 生产者对象
   */
  val producer: KafkaProducer[String, String] = createProducer()

  /**
   * 创建生产者对象
   * ProducerConfig
   */
  def createProducer(): KafkaProducer[String, String] = {
    val producerConfigs = new util.HashMap[String, AnyRef]
    //kafka集群位置
//    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092,node2:9092,node3:9092")
    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, MyPropsUtils(MyConfig.KAFKA_BOOTSTRAP_SERVER))
    producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    //ack
    producerConfigs.put(ProducerConfig.ACKS_CONFIG, "all")
    //幂等配置,默认为false
    producerConfigs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")

    val produce = new KafkaProducer[String, String](producerConfigs)
    produce
  }

  /**
   * 生产(按照默认的黏性分区策略
   */

  def send(topic: String, msg: String): Unit = {
    producer.send(new ProducerRecord[String, String](topic, msg))
  }

  /**
   * 生产（按照key进行分区）
   */

  def send(topic: String, key: String, msg: String): Unit = {
    producer.send(new ProducerRecord[String, String](topic, key, msg))
  }

  /**
   * 关闭生产者对象
   */
  def close():Unit={
    if(producer != null) producer.close()
  }
}
