//1.Count  the  total  number  of  questions  in  the  available  data-set  
// and  collect  the  questions  id  of  all  the  questions

package com.tarun.soa

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object QuesCount {
  
  def main(args : Array[String]) {
    
    System.setProperty("spark.sql.warehouse.dir", "file:/Users/tarun_medimi/Documents/Tools/spark/spark-warehouse")
    
    val spark = SparkSession
                .builder
                .appName("QuesCount")
                .master("local")
                .getOrCreate()
    
                
    val data = spark.read.textFile("/Users/tarun_medimi/Documents/DataSet/Posts.xml").rdd
    
    val result = data.filter(line => line.trim().startsWith("<row"))
    .filter( line => line.contains("PostTypeId=\"1\""))
    
    result.foreach(println)
    println("Total Count: " + result.count())
    
    spark.stop
    
  }
  
}
