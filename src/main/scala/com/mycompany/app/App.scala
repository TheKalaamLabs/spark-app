package com.mycompany.app.sparkapplication
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.apache.spark.SparkContext._

object App {
  println("Hello World!")

  def main(args: Array[String]): Unit = {
    
    val inputfile = args(0)
    val output = args(1)
    
    val conf = new SparkConf().setAppName("Sample")
    val sc = new SparkContext(conf)
    val text_file_rdd = sc.textFile(inputfile)
    val splitted_rdd = text_file_rdd.map(line => line.split(","))
    val key_value_rrd = splitted_rdd.map(x => (x(0), x(1).toFloat))
    val group_rdd = key_value_rrd.reduceByKey((a, b) => a + b)
    val fifty_plus_rdd = group_rdd.filter(x => x._2 > 50)
    fifty_plus_rdd.saveAsTextFile(output)
  }

}

