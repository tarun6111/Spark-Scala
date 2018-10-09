package com.tarun.stb

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.xml.XML

object MinMaxDuration {

  def main(args : Array[String]) {
    
    System.setProperty("spark.sql.warehouse.dir","file:/Users/tarun.madimi/Downloads/spark-2.2.0-bin-hadoop2.6/spark-warehouse")
    
    val spark = SparkSession
                .builder
                .appName("MinMaxDuration")
                .master("local")
                .getOrCreate()
    
    val data = spark.read.textFile("/Users/tarun.madimi/Desktop/Adhoc/Scala/DataSet/Set_Top_Box_Data.txt").rdd
    
    val result = data.filter(line => line.split("\\^")(2).toInt == 118).map( line => {
       val rec = XML.loadString(line.split("\\^")(4))
       val rec1 = rec \\ "nv"

       var duration = 0 
       for (i <- 0 to rec1.length-1 if rec1(i).toString.contains("Duration")) duration = (rec1(i) \\ "@v").toString.toInt;
       
       (duration)
    })
    
    val output = (result.max,result.min)
    
    println("Max duration : " + result.max)
    println("Min duration : " + result.min)
    println("Output : " + output)
    spark.stop
    
  }      
  
}