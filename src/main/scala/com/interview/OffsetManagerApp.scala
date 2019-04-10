package com.interview

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

class OffsetManagerApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("OffsetManagerApp")
    val ssc = new StreamingContext(conf, Seconds(10))



    ssc.start()
    ssc.awaitTermination()
  }
}
