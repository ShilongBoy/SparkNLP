package com.demo.cn.streaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD

object SparkStreamKafka {

  val checkDir="E:\\file\\SparkCheckpoint"

  def functionToCreateContext(): StreamingContext = {
    val conf=new SparkConf()
      .setAppName("SparkStreamKafka")
//      .setMaster("local[*]")
    val sc=new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(1))   // new context
    ssc.checkpoint(checkDir)   // set logDatacheckpoint directory
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "10.1.69.11:6667,10.2.69.12:6667",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "SparkStreamKafka",
      "auto.offset.reset" -> "largest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("kmeansTest")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    val kmodel=KMeansModel.load(sc,"file:///D:\\tmp\\clusters")
    stream.map(record => (record.key, record.value))
      .foreachRDD(x=>{
        val y=x.filter(x=>{x._2.nonEmpty})
        val predictString=y.map(x=>{Vectors.dense(x._2.split(" ").map(_.toDouble))})
        val predictValue=kmodel.predict(predictString).first()
        y.foreach(x=>{println(s"the key is :${x._1} and the values is :${x._2},predictValue:${predictValue}")})
      })
    ssc
  }


  def main(args: Array[String]): Unit = {

    val ssc = StreamingContext.getOrCreate(checkDir, () => functionToCreateContext())
    ssc.start()
    ssc.awaitTermination()
  }

}
