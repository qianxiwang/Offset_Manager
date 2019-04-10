package com.interview

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * offset没管理：ZK/Hbase/Redis
  */
class OffsetManagerApp extends Logging {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("OffsetManagerApp")
    val ssc = new StreamingContext(conf, Seconds(10))

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> "macbookpro:9092",
      "auto.offset.reset" -> "smallest",
      "group.id" -> "pkgroup"
    )

    val topics = "pkoffset".split(",").toSet

    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)


    stream.foreachRDD(rdd => {

      logWarning("pk哥统计结果：" + rdd.count())
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
