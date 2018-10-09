package com.tarun.stb

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.xml.XML

object MaxPriceByOfferId {

  def main(args : Array[String]) {
    
    System.setProperty("spark.sql.warehouse.dir","file:/Users/tarun.madimi/Downloads/spark-2.2.0-bin-hadoop2.6/spark-warehouse")
    
    val spark = SparkSession
                .builder
                .appName("MaxPriceByOfferId")
                .master("local")
                .getOrCreate()
    
    val data = spark.read.textFile("/Users/tarun.madimi/Desktop/Adhoc/Scala/DataSet/Set_Top_Box_Data.txt").rdd
    
    val result = data.filter(line => line.split("\\^")(2).toInt == 102 || line.split("\\^")(2).toInt == 103).map( line => {
       val rec = XML.loadString(line.split("\\^")(4))
       val rec1 = rec \\ "nv"

       var offer = ""
       for (i <- 0 to rec1.length-1 if rec1(i).toString.contains("OfferId")) offer = (rec1(i) \\ "@v").toString;

       var price = 0.00
       for (i <- 0 to rec1.length-1 if rec1(i).toString.contains("Price")) price = (rec1(i) \\ "@v").toString.toFloat;
       
       (offer,price)
    }).groupByKey().map(line => (line._1,line._2.max))
    
    result.collect.foreach(println)
    spark.stop
    
  }      
  
}
