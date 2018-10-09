// 9.The  most  scored  questionswith  specific  tags  –Top  10  questions  having  tag  hadoop,  spark

package com.tarun.soa

import scala.xml.XML

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object MostScoredQuesSpecificTag {
  
  def main(args : Array[String]) {
    
    System.setProperty("spark.sql.warehouse.dir", "file:/Users/tarun_medimi/Documents/Tools/spark/spark-warehouse")
    
    val spark = SparkSession
                .builder
                .appName("MostScoredQuesSpecificTag")
                .master("local")
                .getOrCreate()
    
    val data = spark.read.textFile("/Users/tarun_medimi/Documents/DataSet/Posts.xml").rdd

    val result = data.filter(line => line.trim().startsWith("<row"))
    .filter( line => line.contains("PostTypeId=\"1\""))
    .map( line => {
      val rec = XML.loadString(line)
      (rec.attributes("Tags"),rec.attributes("Score").toString.toInt,line)
    })
    .filter( line => {      
      line._1.toString.contains("hadoop") || line._1.toString.contains("spark")
    })
    .map(line => (line._2,line._3))
    .sortByKey(false)
    
    result.take(10).foreach(println)
    println("Total Count :" + result.count)
    
    spark.stop
    
  }    
  
}