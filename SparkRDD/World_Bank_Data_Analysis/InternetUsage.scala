package com.tarun.wbi

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object InternetUsage {
  
  def main(args : Array[String]) {
    
    val spark = SparkSession
                .builder
                .appName("InternetUsage")
                .getOrCreate()
    
    val data = spark.read.csv(args(0)).rdd
    
    val result = data.filter( line => line.getString(5) != null)
    .map( line => {
     (line.getString(0),line.getString(5).toInt)
    })
    .sortBy(rec => rec._2,false)
    .first
    
    spark.sparkContext.parallelize(Seq(result),1).saveAsTextFile(args(1))
    
    spark.stop
                
  }
  
}