// 7. Number  of  questions  which  are  active  for  last  6  months
package com.tarun.soa

import scala.xml.XML

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat

object ActiveQues {
  
  def main(args : Array[String]) {
    
    System.setProperty("spark.sql.warehouse.dir", "file:/Users/tarun_medimi/Documents/Tools/spark/spark-warehouse")
    
    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
    
    val spark = SparkSession
                .builder
                .appName("ActiveQues")
                .master("local")
                .getOrCreate()
    
    val data = spark.read.textFile("/Users/tarun_medimi/Documents/DataSet/Posts.xml").rdd

    val result = data.filter(line => line.trim().startsWith("<row"))
    .filter( line => line.contains("PostTypeId=\"1\""))
    .map( line => {
      val rec = XML.loadString(line)
      val eDate = format.parse(rec.attributes("LastActivityDate").toString)
      val sDate = format.parse(rec.attributes("CreationDate").toString)
      val eTime = eDate.getTime
      val sTime = sDate.getTime
      val timeDiff = (eTime - sTime)/(1000*60*60*24)
      (timeDiff,eDate,sDate,line)
    })
    .filter(line => line._1>30*6)
    
    result.foreach(println)
    println("Total Active Ques for more than 6 Months :" + result.count)
    spark.stop

   }
}
