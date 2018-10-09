package com.tarun.wbi

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object YoungestCountry {
  
  def main(args : Array[String]) {
    
    val spark = SparkSession
                .builder
                .appName("YoungestCountry")
                .getOrCreate()
    
    val data = spark.read.csv(args(0)).rdd
    
    val wrk1 = data.filter( line => line.getString(15) != null).map( line => {
     (line.getString(0),line.getString(15).toInt)
    })
    .groupByKey
    .map ( line => (line._1,(line._2.sum/line._2.toList.length)))
    .sortBy( rec => rec._2,false)

    val wrk2 = spark.sparkContext.parallelize(wrk1.take(10).toSeq)

    val wrk3 = data.filter( line => line.getString(15) != null ).map( line => {
    (line.getString(0),(line.getString(1),line.getString(15)))
    })

    val wrk4 = wrk2.join(wrk3).map( line => ( line._1, line._2._2._1, line._2._2._2))
    
    wrk4.repartition(1).saveAsTextFile(args(1))
    
    spark.stop
    
  }
  
}
