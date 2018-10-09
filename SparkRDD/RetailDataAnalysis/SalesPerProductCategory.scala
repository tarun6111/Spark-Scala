package com.tarun.rda

import scala.math.random
 
import org.apache.spark.sql.SparkSession

object SalesPerProductCategory {

  def main(args: Array[String]) {
       
        if (args.length < 2) {
      System.err.println("Usage: JavaWordCount <Input-File> <Output-File>");
      System.exit(1);
    }
         
        val spark = SparkSession
                .builder
                .appName("SalesPerProductCategory")
                .getOrCreate()
 
    val data = spark.read.textFile(args(0)).rdd
    
    val salesByProdCat = data.map(line => {
      (line.split("\t")(3),line.split("\t")(4).toFloat)
      
    })
    .reduceByKey(_+_)

    salesByProdCat.saveAsTextFile(args(1))
     
    spark.stop
  
}

}

//spark-submit --class com.tarun.rda.SalesPerProductCategory /Users/tarun.madimi/Desktop/Adhoc/Scala/MyJars/rda_1.jar /Users/tarun.madimi/Desktop/Adhoc/Scala/DataSet/Retail_Sample_Data_Set.txt rda-out-0001