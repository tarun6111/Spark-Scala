val data = spark.read.csv("olympic_Data.csv")

data.createOrReplaceTempView("olympic")

#KPI-1: No of athletes participated in each Olympic event
scala> spark.sql("SELECT _c3,count(1) FROM olympic GROUP BY _c3").show
+----+--------+
| _c3|count(1)|
+----+--------+
|2012|    1776|
|2000|    1840|
|2002|     407|
|2006|     443|
|2004|    1839|
|2008|    1872|
|2010|     441|
+----+--------+

#KPI-2: No of medals each country won in each Olympic in ascending order
scala> spark.sql("SELECT _c2,_c3,SUM(_c9) as cnt FROM olympic GROUP BY _c2,_c3 ORDER BY _c2,_c3").show
+-----------+----+-----+
|        _c2| _c3|  cnt|
+-----------+----+-----+
|Afghanistan|2008|  1.0|
|Afghanistan|2012|  1.0|
|    Algeria|2000|  5.0|
|    Algeria|2008|  2.0|
|    Algeria|2012|  1.0|
|  Argentina|2000| 20.0|
|  Argentina|2004| 49.0|
|  Argentina|2008| 51.0|
|  Argentina|2012| 21.0|
|    Armenia|2000|  1.0|
|    Armenia|2008|  6.0|
|    Armenia|2012|  3.0|
|  Australia|2000|183.0|
|  Australia|2002|  2.0|
|  Australia|2004|156.0|
|  Australia|2006|  2.0|
|  Australia|2008|149.0|
|  Australia|2010|  3.0|
|  Australia|2012|114.0|
|    Austria|2000|  4.0|
+-----------+----+-----+
only showing top 20 rows

#KPI-3: Top 10 athletes who won highest gold medals in all the Olympic events
scala> spark.sql("SELECT _c0,SUM(_c6) as cnt FROM olympic GROUP BY _c0 ORDER BY cnt desc limit 10").show
+--------------------+----+
|                 _c0| cnt|
+--------------------+----+
|      Michael Phelps|18.0|
|           Chris Hoy| 6.0|
|          Usain Bolt| 6.0|
|         Ryan Lochte| 5.0|
|Georgeta Damian-A...| 5.0|
|   Valentina Vezzali| 5.0|
|             Zou Kai| 5.0|
| Anastasiya Davydova| 5.0|
|          Ian Thorpe| 5.0|
|Ole Einar Bj�rndalen| 5.0|
+--------------------+----+

#KPI-4: No of athletes who won gold and whose age is less than 20
scala> spark.sql("SELECT COUNT(*) FROM olympic WHERE (_c6 > 0) AND (_c1<20)").show
+--------+
|count(1)|
+--------+
|     188|
+--------+

#KPI-5: Youngest athlete who won gold in each category of sports in each Olympic
scala> spark.sql("SELECT * FROM olympic WHERE (_c1=(SELECT min(_c1) FROM olympic WHERE (_c6>0) AND (_c1>0))) AND (_c6>0)").show
+--------------+---+-------------+----+---------+--------------------+---+---+---+---+
|           _c0|_c1|          _c2| _c3|      _c4|                 _c5|_c6|_c7|_c8|_c9|
+--------------+---+-------------+----+---------+--------------------+---+---+---+---+
|    Yang Yilin| 15|        China|2008|8/24/2008|          Gymnastics|  1|  0|  2|  3|
|   Go Gi-Hyeon| 15|  South Korea|2002|2/24/2002|Short-Track Speed...|  1|  1|  0|  2|
|   Chen Ruolin| 15|        China|2008|8/24/2008|              Diving|  2|  0|  0|  2|
| Katie Ledecky| 15|United States|2012|8/12/2012|            Swimming|  1|  0|  0|  1|
|Ruta Meilutyte| 15|    Lithuania|2012|8/12/2012|            Swimming|  1|  0|  0|  1|
|Olga Glatskikh| 15|       Russia|2004|8/29/2004| Rhythmic Gymnastics|  1|  0|  0|  1|
|     Kyla Ross| 15|United States|2012|8/12/2012|          Gymnastics|  1|  0|  0|  1|
+--------------+---+-------------+----+---------+--------------------+---+---+---+---+

#KPI-6: No of atheletes from each country who has won a medal in each Olympic in each sports
scala> spark.sql("SELECT _c2,_c3,_c5,count(1) FROM olympic WHERE (_c6>0) OR (_c7>0) OR (_c8>0) GROUP BY _c2,_c3,_c5 ORDER BY _c2,_c3").show 
+-----------+----+----------+--------+
|        _c2| _c3|       _c5|count(1)|
+-----------+----+----------+--------+
|Afghanistan|2008| Taekwondo|       1|
|Afghanistan|2012| Taekwondo|       1|
|    Algeria|2000| Athletics|       4|
|    Algeria|2000|    Boxing|       1|
|    Algeria|2008|      Judo|       2|
|    Algeria|2012| Athletics|       1|
|  Argentina|2000|   Sailing|       4|
|  Argentina|2000|    Hockey|      16|
|  Argentina|2004|  Swimming|       1|
|  Argentina|2004|   Sailing|       2|
|  Argentina|2004|Basketball|      12|
|  Argentina|2004|    Hockey|      16|
|  Argentina|2004|  Football|      16|
|  Argentina|2004|    Tennis|       2|
|  Argentina|2008|   Sailing|       2|
|  Argentina|2008|   Cycling|       2|
|  Argentina|2008|  Football|      18|
|  Argentina|2008|Basketball|      12|
|  Argentina|2008|      Judo|       1|
|  Argentina|2008|    Hockey|      16|
+-----------+----+----------+--------+
only showing top 20 rows

#KPI-7: No of athletes won at least a medal in each events in all the Olympics
spark.sql("SELECT DISTINCT _c5, count(*) as cnt FROM olympic group by _c5 order by cnt DESC").show

#KPI-8: Country won highest no of medals in wrestling in 2012
spark.sql("SELECT _c2,SUM(_c9) as cnt FROM olympic WHERE (_c5='Wrestling') AND (_c3=2008) GROUP BY _c2 ORDER BY cnt desc").show
