// 8. Questions  which  are  marked  closedfor  each  category–provide  the  distribution  of  number  of  
// closed  questions  per  month

package com.tarun.soa

import scala.xml.XML

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat


object ClosedQues {
  
  def main(args : Array[String]) {
    
    System.setProperty("spark.sql.warehouse.dir", "file:/Users/tarun_medimi/Documents/Tools/spark/spark-warehouse")
    
    val format  = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
    val format2 = new SimpleDateFormat("yyyy-MM")
    
    val spark = SparkSession
                .builder
                .appName("ClosedQues")
                .master("local")
                .getOrCreate()
    
    val data = spark.read.textFile("/Users/tarun_medimi/Documents/DataSet/Posts.xml").rdd

    val result = data.filter(line => line.trim().startsWith("<row"))
    .filter( line => line.contains("PostTypeId=\"1\""))
    .filter( rec => XML.loadString(rec).attributes("ClosedDate") != null)
    .map( line => {
      val rec = XML.loadString(line).attributes("ClosedDate").toString
      val closeDate = format2.format(format.parse(rec))
      (closeDate,1)
    })
    .reduceByKey(_+_)
    
    result.foreach(println)
    spark.stop

  }
}
