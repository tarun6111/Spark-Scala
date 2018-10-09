val data = spark.read.csv("titanic_data.txt")

data.createOrReplaceTempView("data")

#KPI-1: Find the average age of people who died and who survived
scala> spark.sql("SELECT avg(_c4) FROM data where (_c2 = 1) and (_c4 != 'NA')").show
+------------------------+
|avg(CAST(_c4 AS DOUBLE))|
+------------------------+
|      29.873961921708187|
+------------------------+
scala> spark.sql("SELECT avg(_c4) FROM data where (_c2 = 0) and (_c4 != 'NA')").show
+------------------------+
|avg(CAST(_c4 AS DOUBLE))|
+------------------------+
|       32.24810596590909|
+------------------------+

#KPI-2:	Number of males and females survived in following age range: (age <= 20), (20 < age <= 50) and (age > 50 and age = NA)
scala> spark.sql("SELECT _c10,COUNT(1) from data where _c2=1 and _c4 <=20 GROUP BY _c10").show
+------+--------+
|  _c10|count(1)|
+------+--------+
|female|      38|
|  male|       8|
+------+--------+
scala> spark.sql("SELECT _c10,COUNT(1) from data where (_c2 = 1) and (_c4 > 20) and (_c4 <= 50) GROUP BY _c10").show
+------+--------+
|  _c10|count(1)|
+------+--------+
|female|     125|
|  male|      49|
+------+--------+
scala> spark.sql("SELECT _c10,COUNT(1) from data where (_c2 = 1) and ((_c4 > 50) OR (_c4 ='NA')) GROUP BY _c10").show
+------+--------+
|  _c10|count(1)|
+------+--------+
|female|     131|
|  male|      65|
+------+--------+

#KPI-3	embarked locations and their count
scala> spark.sql("SELECT _c5,COUNT(1) FROM data WHERE (_c5 IS NOT NULL) GROUP BY _c5").show
+-----------+--------+
|        _c5|count(1)|
+-----------+--------+
| Queenstown|      45|
|Southampton|     573|
|  Cherbourg|     203|
+-----------+--------+

#KPI-4:	Number of people survived in each class
scala> spark.sql("SELECT _c1,COUNT(1) FROM data GROUP BY _c1").show
+---+--------+
|_c1|count(1)|
+---+--------+
|2nd|     280|
|1st|     322|
|3rd|     711|
+---+--------+

#KPI-5	Number of males survived whose age is less than 30 and travelling in 2nd class
scala> spark.sql("SELECT COUNT(1) FROM data WHERE (_c10='male') AND (_c2=1) AND (_c4 != 'NA') AND (_c4 < 30) AND (_c1='2nd')").show
+--------+
|count(1)|
+--------+
|       5|
+--------+
