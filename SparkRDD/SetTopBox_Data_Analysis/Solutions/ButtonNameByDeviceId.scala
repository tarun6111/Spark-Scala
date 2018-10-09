package com.tarun.stb

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.xml.XML

object ButtonNameByDeviceId {

  def main(args : Array[String]) {
    
    System.setProperty("spark.sql.warehouse.dir","file:/Users/tarun.madimi/Downloads/spark-2.2.0-bin-hadoop2.6/spark-warehouse")
    
    val spark = SparkSession
                .builder
                .appName("ButtonNameByDeviceId")
                .master("local")
                .getOrCreate()
    
    val data = spark.read.textFile("/Users/tarun.madimi/Desktop/Adhoc/Scala/DataSet/Set_Top_Box_Data.txt").rdd
    
    val result = data.filter(line => line.split("\\^")(2).toInt == 107).map( line => {
       val rec = XML.loadString(line.split("\\^")(4))
       val rec1 = rec \\ "nv"

       var buttonName = "" 
       for (i <- 0 to rec1.length-1 if rec1(i).toString.contains("ButtonName")) buttonName = (rec1(i) \\ "@v").toString;
 
       (line.split("\\^")(5),buttonName)
       
    }).groupByKey().map( line => (line._1,line._2.toSet))
    
    result.collect.foreach(println)
    spark.stop
    
  }      
  
}
