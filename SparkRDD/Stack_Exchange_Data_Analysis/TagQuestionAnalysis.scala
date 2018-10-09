// 11.Number  of  question  with  specific  tags(nosql,  big  data)  which  was  asked  in  the  
// specified  time  range  (from  01-01-2015  to  31-12-2015)
package com.tarun.soa

import scala.xml.XML

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat

object TagQuestionAnalysis {
  
  def main(args : Array[String]) {
    
    System.setProperty("spark.sql.warehouse.dir", "file:/Users/tarun_medimi/Documents/Tools/spark/spark-warehouse")
    
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val format2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
    
    val startTime = format.parse("2015-01-01").getTime
    val endTime   = format.parse("2015-12-31").getTime
    
    val spark = SparkSession
                .builder
                .appName("MostScoredQuesSpecificTag")
                .master("local")
                .getOrCreate()
    
    val data = spark.read.textFile("/Users/tarun_medimi/Documents/DataSet/Posts.xml").rdd

    val result = data.filter(line => line.trim().startsWith("<row"))
    .filter( line => line.contains("PostTypeId=\"1\""))
    .filter( line => {
      XML.loadString(line).attributes("Tags") != null && 
      (XML.loadString(line).attributes("Tags").toString.toLowerCase.contains("nosql") ||
       XML.loadString(line).attributes("Tags").toString.toLowerCase.contains("bigdata"))
    })
    .filter( line => {
      val rec = format2.parse(XML.loadString(line).attributes("CreationDate").toString).getTime
      
      rec > startTime && rec < endTime
    })
    
    println("Total number of questions :" + result.count)
    spark.stop
  
  }  
}