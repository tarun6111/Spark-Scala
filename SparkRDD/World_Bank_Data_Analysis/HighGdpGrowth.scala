package com.tarun.wbi

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object HighGdpGrowth {
  
  def main (args : Array[String]) {
    
    val spark = SparkSession
                .builder
                .appName("HighGdpGrowth")
                .getOrCreate()
    
    val data = spark.read.csv(args(0)).rdd
    
    val result = data.filter(line => line.getString(18) != null)
    .filter( line => {
     line.getString(1).split("/")(2).toInt >=2009
    })
    .map( line => {
     (line.getString(0),line.getString(18).replaceAll(",", "").toLong)
    })
    .groupByKey
    .map( line => {
     val gdpList = line._2.toList
     var gdpGrowth = 0f
     if(gdpList.length == 2) { 
      gdpGrowth =(gdpList(1) - gdpList(0))/gdpList(0).toFloat*100
     }
     (line._1,gdpGrowth)
    })
    .sortBy( rec => rec._2,false)
    .take(10)
    
    spark.sparkContext.parallelize(result.toSeq,1).saveAsTextFile(args(1))
    
    spark.stop
    
  }
  
}