// 10.List  of  all  the  tags  along  with  their  counts
package com.tarun.soa

import scala.xml.XML

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TagAnalysis {
  
  def main(args : Array[String]) {
    
    System.setProperty("spark.sql.warehouse.dir", "file:/Users/tarun_medimi/Documents/Tools/spark/spark-warehouse")
    
    val spark = SparkSession
                .builder
                .appName("TagAnalysis")
                .master("local")
                .getOrCreate()
    
    val data = spark.read.textFile("/Users/tarun_medimi/Documents/DataSet/Posts.xml").rdd

    val result = data.filter(line => line.trim().startsWith("<row"))
    .filter( line => XML.loadString(line).attributes("Tags") != null)
    .map( line => {
      val rec = XML.loadString(line)
      (rec.attributes("Tags").toString.replaceAll("&lt;", "").replaceAll("&gt;", ""),1)
    })
    .reduceByKey(_+_)
    .sortByKey(true)
    
    result.foreach(println)
    spark.stop
    
  }  
}
