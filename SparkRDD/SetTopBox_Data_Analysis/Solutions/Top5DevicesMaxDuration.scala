package com.tarun.stb

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.xml.XML

object Top5DevicesMaxDuration {
  
  def main(args : Array[String]) {
    
    val spark = SparkSession
                .builder
                .appName("Top5DevicesMaxDuration")
                .getOrCreate()
    
    val data = spark.read.textFile(args(0)).rdd
    
    val result = data.filter(line => line.split("\\^")(2).toInt == 100).filter( line => {
       val rec = XML.loadString(line.split("\\^")(4))
       val rec1 = rec \\ "nv"
       rec1.length == 8
    }).map( line => {
       val rec = XML.loadString(line.split("\\^")(4))
       val rec1 = rec \\ "nv"
       var duration = 0 
       for (i <- 0 to rec1.length-1 if rec1(i).toString.contains("Duration")) duration = (rec1(i) \\ "@v").toString.toInt;
 
       (line.split("\\^")(5),duration)
    }).sortBy( rec => rec._2, false).take(5)
    
    spark.sparkContext.parallelize(result.toSeq,1).saveAsTextFile(args(1))
    spark.stop
    
  }
  
}
//spark-submit --class com.tarun.stb.Top5DevicesMaxDuration /Users/tarun.madimi/Desktop/Adhoc/Scala/MyJars/stb_data.jar /Users/tarun.madimi/Desktop/Adhoc/Scala/DataSet/Set_Top_Box_Data.txt stb-out-001
