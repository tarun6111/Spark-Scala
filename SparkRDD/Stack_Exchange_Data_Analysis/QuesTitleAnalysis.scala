//3. Provide  the  number  of  posts  which  are  questions  and  contains  specified  words  in  their  
// title(like  data,  science,  nosql,  hadoop,  spark)
package com.tarun.soa

import scala.xml.XML

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object QuesTitleAnalysis {

  def main(args : Array[String]) {
    
    System.setProperty("spark.sql.warehouse.dir", "file:/Users/tarun_medimi/Documents/Tools/spark/spark-warehouse")
    
    val spark = SparkSession
                .builder
                .appName("QuesTitleAnalysis")
                .master("local")
                .getOrCreate()
    
    val data = spark.read.textFile("/Users/tarun_medimi/Documents/DataSet/Posts.xml").rdd

    val result = data.filter(line => line.trim().startsWith("<row"))
    .filter( line => line.contains("PostTypeId=\"1\""))
    .map( line => {
      val rec = XML.loadString(line)
      rec.attributes("Title")
    })
    .filter( line => {      
      line.toString.contains("hadoop") || line.toString.contains("spark")
    })
    
    result.foreach(println)
    println("Total Count :" + result.count)
    
    spark.stop
    
  }  
  
}