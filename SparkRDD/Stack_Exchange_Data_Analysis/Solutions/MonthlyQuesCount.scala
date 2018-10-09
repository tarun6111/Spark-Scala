//2.Monthly  questions  count  –provide  the  distribution  of  number  of  questions  asked  per  month

package com.tarun.soa

import scala.xml.XML

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object MonthlyQuesCount {
  
  def main(args : Array[String]) {
    
    System.setProperty("spark.sql.warehouse.dir", "file:/Users/tarun_medimi/Documents/Tools/spark/spark-warehouse")
    
    val spark = SparkSession
                .builder
                .appName("MonthlyQuesCount")
                .master("local")
                .getOrCreate()
    
    val data = spark.read.textFile("/Users/tarun_medimi/Documents/DataSet/Posts.xml").rdd

    val result = data.filter(line => line.trim().startsWith("<row"))
    .filter( line => line.contains("PostTypeId=\"1\""))
    .map( line => {
      val rec = XML.loadString(line)
      (rec.attributes("CreationDate").toString.substring(0,7),1)      
    })
    .reduceByKey(_ + _)
    
    result.foreach(println)
    
    spark.stop

  }
  
}
