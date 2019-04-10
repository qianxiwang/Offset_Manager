package com.interview


import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import scalikejdbc.config.DBs
import scalikejdbc._


/**
  * offset没管理：ZK/Hbase/Redis
  */
class OffsetManagerApp extends Logging {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("OffsetManagerApp")
    val ssc = new StreamingContext(conf, Seconds(10))

    DBs.setup()

    val fromOffsets = DB.readOnly{ implicit  session => {
      sql"select * from offsets_storage".map(rs => {
        (TopicAndPartition(rs.string("topic"),rs.int("partitions")), rs.long("offset"))
      }).list().apply()
    }
    }.toMap


    for (ele <- fromOffsets) {
    logWarning("From Mysql:" + ele._1.topic + " : " + ele._1.partitions + " : " + ele._2)
  }

  val kafkaParams = Map[String, String](
    "metadata.broker.list" -> "macbookpro:9092",
    "auto.offset.reset" -> "smallest",
    "group.id" -> "pkgroup"
  )

  val topics = "pkoffset".split(",").toSet

  val stream = if (fromOffsets.size == 0) {
    //第一次就是从头开始消费
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
  } else { //非第一次，是从某个地方开始消费

    //      val fromOffsets = Map[TopicAndPartition, Long]
    val messageHandler = (mm: MessageAndMetadata[String, String]) => (mm.key(), mm.message())
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
  }

  stream.foreachRDD(rdd => {
    logWarning("PK哥统计结果：" + rdd.count())

    var offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

    for (o <- offsetRanges) {
      println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")

      DB.autoCommit {
        implicit session => {
          sql"replace into offset_storage(topic,groupid,partitions,offset) values(?,?,?,?)"
            .bind(o.topic, "pkgroup", o.partition, o.untilOffset)
            .update().apply()
        }
      }

    }
  })


  ssc.start()
  ssc.awaitTermination()
}

}
