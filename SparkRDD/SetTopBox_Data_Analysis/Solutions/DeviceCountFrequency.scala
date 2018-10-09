package com.tarun.stb

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.xml.XML

object DeviceCountFrequency {

  def main(args : Array[String]) {
    
    val spark = SparkSession
                .builder
                .appName("Top5DevicesMaxDuration")
                .getOrCreate()
    
    val data = spark.read.textFile(args(0)).rdd
    
    val result = data.filter(line => line.split("\\^")(2).toInt == 115 || line.split("\\^")(2).toInt == 118).map( line => {
       val rec = XML.loadString(line.split("\\^")(4))
       val rec1 = rec \\ "nv"
       var duration = 0 
       for (i <- 0 to rec1.length-1 if rec1(i).toString.contains("Duration")) duration = (rec1(i) \\ "@v").toString.toInt;

       var programId = "" 
       for (i <- 0 to rec1.length-1 if rec1(i).toString.contains("ProgramId")) programId = (rec1(i) \\ "@v").toString;
       
       (programId,duration)
    }).groupByKey()
    
    
    spark.stop
    
  }
  
}
