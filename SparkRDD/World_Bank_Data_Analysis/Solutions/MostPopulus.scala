package com.tarun.wbi

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object MostPopulus {
  
  def main(args : Array[String]) {
    
    if (args.length < 2) {
      System.err.println("Usage : Most Populus <Input-File> <Output-File>");
      System.exit(1);
      
    }
    
    val spark = SparkSession
                .builder
                .appName("MostPopulus")
                .getOrCreate()
    
    val data = spark.read.csv(args(0)).rdd
    
    val result = data.map( line => {
      
      val pop = line.getString(9).replaceAll(",", "")
      val popNum = pop.toLong
      
      (line.getString(0),popNum)  
 
    })
    .groupByKey()
    .map( rec => (rec._1,rec._2.max))
    .sortBy( rec => rec._2, false)
    .take(10)
    
    spark.sparkContext.parallelize(result.toSeq,1).saveAsTextFile(args(1))
    
    spark.stop
    
  }
  
}
