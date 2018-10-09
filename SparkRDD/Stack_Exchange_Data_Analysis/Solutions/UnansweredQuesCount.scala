// 5. The  questions  that  doesn’t  have  any  answers  –Number  of  questions  with  “0”  number  of  answers

package com.tarun.soa

import scala.xml.XML

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object UnansweredQuesCount {
  
  def main(args : Array[String]) {
    
    System.setProperty("spark.sql.warehouse.dir", "file:/Users/tarun_medimi/Documents/Tools/spark/spark-warehouse")
    
    val spark = SparkSession
                .builder
                .appName("UnansweredQuesCount")
                .master("local")
                .getOrCreate()
    
    val data = spark.read.textFile("/Users/tarun_medimi/Documents/DataSet/Posts.xml").rdd

    val result = data.filter(line => line.trim().startsWith("<row"))
    .filter( line => line.contains("PostTypeId=\"1\""))
    .map( line => {
      val rec = XML.loadString(line)
      (rec.attributes("AnswerCount").toString.toInt,line)
    })
    .filter(line => line._1 == 0)
    
    result.foreach(println)
    println("Total unanswered questions :" + result.count)
    spark.stop
  
  }  
  
}
