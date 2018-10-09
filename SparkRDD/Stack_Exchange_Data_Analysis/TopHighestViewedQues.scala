//4. The  trending  questions  which  are  viewed and  scored  highly  by  the  user  
//   –Top  10  highest  viewed  questions  with  specific  tags

package com.tarun.soa

import scala.xml.XML

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TopHighestViewedQues {
  
  def main(args : Array[String]) {
    
    System.setProperty("spark.sql.warehouse.dir", "file:/Users/tarun_medimi/Documents/Tools/spark/spark-warehouse")
    
    val spark = SparkSession
                .builder
                .appName("TopHighestViewedQues")
                .master("local")
                .getOrCreate()
    
    val data = spark.read.textFile("/Users/tarun_medimi/Documents/DataSet/Posts.xml").rdd

    val result = data.filter(line => line.trim().startsWith("<row"))
    .filter( line => line.contains("PostTypeId=\"1\""))
    .map( line => {
      val rec = XML.loadString(line)
      (rec.attributes("ViewCount").toString.toInt,line)
    })
    .filter( line => line._1 != null)
    .sortByKey(false)
    
    result.take(10).foreach(println)
    spark.stop
    
  }  
  
}