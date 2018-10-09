package com.tarun.wbi

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object HighPopuGrowth {
  
  def main( args : Array[String] ) {
    
    val spark = SparkSession
                .builder
                .appName("HighPopuGrowth")
                .getOrCreate()
    
    
    val data = spark.read.csv(args(0)).rdd
    
    val result = data.filter( line => {
     line.getString(1).split("/")(2).toInt == 2000 || line.getString(1).split("/")(2).toInt == 2010
    })
    .map(line => (line.getString(0),line.getString(9).replaceAll(",", "").toLong))
    .groupByKey
    .map( line => {
     val popList = line._2.toList
     var popGrowth = 0f
     if(popList.length == 2) {
      popGrowth = (popList(1)-popList(0))/popList(0).toFloat*100
     }
     (line._1,popGrowth)
    })
    .sortBy( rec => rec._2, false)
    .first
    
    spark.sparkContext.parallelize(Seq(result),1).saveAsTextFile(args(1))
    
    spark.stop
      
  }
  
}
