// 6. Number  of  questions  with  more  than  2  answers

package com.tarun.soa

import scala.xml.XML

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object MoreThanTwoAnsQues {
  
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
      (rec.attributes("AnswerCount").toString.toInt,line)
    })
    .filter(line => line._1 > 2 )
    
    result.foreach(println)
    println("Total questions having more than 2 answers :" + result.count)
    spark.stop
  
  }  
  
}