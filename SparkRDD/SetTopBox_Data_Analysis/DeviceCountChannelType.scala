package com.tarun.stb

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.xml.XML

object DeviceCountChannelType {

  def main(args : Array[String]) {
    
    val spark = SparkSession
                .builder
                .appName("DeviceCountChannelType")
                .getOrCreate()
    
    val data = spark.read.textFile(args(0)).rdd
    
    val result = data.filter(line => line.split("\\^")(2).toInt == 100).filter( line => {
       val rec = XML.loadString(line.split("\\^")(4))
       val rec1 = rec \\ "nv"
       rec1.length == 8
    }).map( line => {
       val rec = XML.loadString(line.split("\\^")(4))
       val rec1 = rec \\ "nv"

       var channelType = ""
       for (i <- 0 to rec1.length-1 if rec1(i).toString.contains("ChannelType")) channelType = (rec1(i) \\ "@v").toString;

       (line.split("\\^")(5),channelType)
    }).filter( line => line._2 == "LiveTVMediaChannel")
    .distinct.count
    
    spark.sparkContext.parallelize(Seq(result),1).saveAsTextFile(args(1))
    spark.stop
    
  }  
  
}
//spark-submit --class com.tarun.stb.DeviceCountChannelType /Users/tarun.madimi/Desktop/Adhoc/Scala/MyJars/stb_data.jar /Users/tarun.madimi/Desktop/Adhoc/Scala/DataSet/Set_Top_Box_Data.txt stb-out-003