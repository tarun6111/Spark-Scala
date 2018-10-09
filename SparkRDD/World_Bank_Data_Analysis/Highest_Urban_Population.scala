package com.tarun.wbi

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache. spark.sql.SparkSession
import java.lang.Long

object Highest_Urban_Population {
  def main(args : Array[String]) = {
    
    if (args.length < 2) {
      System.err.println("Usage : Urban Population <Input-File> <Output-File>");
      System.exit(1);
    }
    
    val spark = SparkSession
                .builder
                .appName("Highest_Urban_Population")
                .getOrCreate()
    
    val data = spark.read.csv(args(0)).rdd
    
    val result = data.filter(line => line.getString(10) != null).map( line => {
      
      val uPop = line.getString(10).replaceAll(",","")
      var uPopNum = 0L
      if(uPop.length() > 0)
        uPopNum = Long.parseLong(uPop)
      
      (uPopNum,line.getString(0))
      
    })
    .sortByKey(false)
    .first
    
    spark.sparkContext.parallelize(Seq(result),1).saveAsTextFile(args(1))
    
    spark.stop
    
  }
  
}