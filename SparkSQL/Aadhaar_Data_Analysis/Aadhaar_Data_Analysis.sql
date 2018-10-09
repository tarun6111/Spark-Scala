val data = spark.read.csv("aadhaar_data.csv")

data.createOrReplaceTempView("aadhaar")

KPI-1:
=====
1. View/result of the top 25 rows from each individual store
spark.sql("""
SELECT * FROM (
SELECT *, rank() OVER (PARTITION BY _c1 ORDER BY _c6 desc) as rnk FROM aadhaar 
) WHERE rnk < 25
""").show

KPI-2:
=====
1. Find the count and names of registrars in the table.
scala> spark.sql("SELECT _c1,count(1) as cnt FROM aadhaar GROUP BY _c1").show(200)
+--------------------+------+                                                   
|                 _c1|   cnt|
+--------------------+------+
|Govt of Andhra Pr...| 47489|
| UT Of Daman and Diu|  1786|
|Govt of Madhya Pr...|  3582|
|Punjab National Bank|  3303|
|NSDL e-Governance...|122709|
|       IDBI Bank ltd|  9218|
|    Delhi - North DC|  4372|
| Govt of Maharashtra|165780|
|           Indiapost| 22116|
|  FCS Govt of Punjab| 29937|
|Govt of Sikkim - ...|   783|
|       Delhi - NE DC|  1516|
| FCR Govt of Haryana|  7959|
|               IGNOU|   124|
|Principal Revenue...|  2157|
|      Syndicate Bank|    25|
|Indian Overseas Bank|    52|
|     Delhi - East DC|  1957|
|Department of Inf...|  3772|
| Bank of Maharashtra|  2723|
|           DENA BANK|103801|
| State Bank of India| 11139|
|Punjab and Sind Bank|  1345|
|         Indian Bank|     1|
|Registrar General...|  3846|
|         Delhi SW DC|  1692|
|  Govt of Karnataka | 47894|
|             N S D L|  7326|
|         Canara Bank| 19857|
|Central Bank of I...| 17706|
|      Govt of Kerala| 54511|
|Registrar General...| 59641|
|           Jharkhand| 28199|
|CSC e-Governance ...|242637|
|      Allahabad Bank| 28752|
|          Union Bank|112175|
|United Bank of India|    20|
|Registrar General...| 73960|
| RDD Govt of Tripura|   484|
|     Delhi- South DC|  1777|
|State Bank of Tra...|   239|
|Oriental Bank of ...| 14202|
|Mission Convergen...|  2523|
|Information Techn...| 54968|
|         Delhi-NW DC|  1810|
|UTI Infrastructur...|   260|
|Atalji Janasnehi ...|  8122|
|UDD Govt of Jhark...|  3566|
|      Bank of Baroda|  5179|
|Registrar General...|   491|
|State Bank of Bik...|  8286|
|     Govt of Gujarat| 63277|
|Govt of Himachal ...| 18225|
|STATE BANK OF HYD...|     4|
|    UT of Puducherry|   630|
|         Govt of Goa|  5222|
|Delhi Urban Shelt...|   790|
|     UIDAI-Registrar|    89|
|Rural Development...|  3516|
|      Delhi- West DC|  2670|
|  Delhi - Central DC|  1113|
|       Bank Of India| 83031|
|Civil Supplies - ...|   394|
|Registrar General...| 70159|
|       Delhi - ND DC|   191|
|Life Insurance Co...|    55|
|Dept of ITC Govt ...| 63434|
|STATE BANK OF PAT...|   165|
|Project Coordinat...|  5213|
+--------------------+------+

2. Find the number of states, districts in each state and sub-districts in each district.
scala> spark.sql("SELECT COUNT(distinct(_c3)) as no_of_states FROM aadhaar").show
+------------+                                                                  
|no_of_states|
+------------+
|          37|
+------------+

scala> spark.sql("SELECT _c3,COUNT(distinct(_c4)) as no_of_districts FROM aadhaar GROUP BY _c3").show(40)
+--------------------+---------------+
|                 _c3|no_of_districts|
+--------------------+---------------+
|            Nagaland|              9|
|           Karnataka|             44|
|              Odisha|             33|
|              Kerala|             15|
|          Tamil Nadu|             40|
|        Chhattisgarh|             34|
|      Andhra Pradesh|             29|
|         Lakshadweep|              1|
|      Madhya Pradesh|             50|
|              Punjab|             25|
|             Manipur|              9|
|                 Goa|              7|
|             Mizoram|              7|
|Dadra and Nagar H...|              1|
|    Himachal Pradesh|             13|
|          Puducherry|              4|
|             Haryana|             21|
|   Jammu and Kashmir|             24|
|           Jharkhand|             29|
|   Arunachal Pradesh|             16|
|             Gujarat|             33|
|              Sikkim|              4|
|               Delhi|             11|
|              Others|              1|
|          Chandigarh|              2|
|           Rajasthan|             34|
|Andaman and Nicob...|              4|
|               Assam|             27|
|           Meghalaya|              6|
|       Daman and Diu|              2|
|         Maharashtra|             42|
|         West Bengal|             29|
|           Telangana|             10|
|               Bihar|             40|
|             Tripura|              8|
|       Uttar Pradesh|             81|
|         Uttarakhand|             15|
+--------------------+---------------+

scala> spark.sql("SELECT _c3,_c4,COUNT(distinct(_c5)) as no_of_subdistricts FROM aadhaar GROUP BY _c3,_c4 ORDER BY _c3,_c4").show
+--------------------+--------------------+------------------+                  
|                 _c3|                 _c4|no_of_subdistricts|
+--------------------+--------------------+------------------+
|Andaman and Nicob...|            Andamans|                 5|
|Andaman and Nicob...|             Nicobar|                 1|
|Andaman and Nicob...|North And Middle ...|                 3|
|Andaman and Nicob...|       South Andaman|                 3|
|      Andhra Pradesh|            Adilabad|                54|
|      Andhra Pradesh|           Anantapur|                63|
|      Andhra Pradesh|          Ananthapur|                63|
|      Andhra Pradesh|            Chittoor|                68|
|      Andhra Pradesh|            Cuddapah|                53|
|      Andhra Pradesh|       East Godavari|                62|
|      Andhra Pradesh|              Guntur|                59|
|      Andhra Pradesh|           Hyderabad|                17|
|      Andhra Pradesh|      K.V.Rangareddy|                29|
|      Andhra Pradesh|     K.v. Rangareddy|                38|
|      Andhra Pradesh|         Karim Nagar|                 9|
|      Andhra Pradesh|          Karimnagar|                59|
|      Andhra Pradesh|             Khammam|                50|
|      Andhra Pradesh|             Krishna|                51|
|      Andhra Pradesh|             Kurnool|                55|
|      Andhra Pradesh|       Mahabub Nagar|                41|
+--------------------+--------------------+------------------+
only showing top 20 rows

3. Find the number of males and females in each state from the table.
scala> spark.sql("SELECT _c3,sum(case when _c7='M' THEN 1 ELSE 0 END) as no_of_males,sum(case when _c7='F' THEN 1 ELSE 0 END) as no_of_females FROM aadhaar GROUP BY _c3").show
+--------------------+-----------+-------------+                                
|                 _c3|no_of_males|no_of_females|
+--------------------+-----------+-------------+
|            Nagaland|        792|          380|
|           Karnataka|      44916|        44599|
|              Odisha|      12213|        10997|
|              Kerala|      41801|        44411|
|          Tamil Nadu|      28359|        16606|
|        Chhattisgarh|      10189|        13349|
|      Andhra Pradesh|      74748|        80768|
|         Lakshadweep|          9|            7|
|      Madhya Pradesh|      56623|        53619|
|              Punjab|      25494|        23357|
|             Manipur|       3651|          568|
|                 Goa|       2780|         3352|
|             Mizoram|         86|           87|
|Dadra and Nagar H...|         91|           41|
|    Himachal Pradesh|      10032|         9103|
|          Puducherry|        532|          406|
|             Haryana|      21890|        25035|
|   Jammu and Kashmir|       2715|         2653|
|           Jharkhand|      32005|        31394|
|   Arunachal Pradesh|        273|           89|
+--------------------+-----------+-------------+
only showing top 20 rows

4. Find out the names of private agencies for each state
scala> spark.sql("SELECT _c3,_c2 FROM aadhaar GROUP BY _c3,_c2 ORDER BY _c3,_c2").show
+--------------------+--------------------+                                     
|                 _c3|                 _c2|
+--------------------+--------------------+
|Andaman and Nicob...|A3 Logics  India ...|
|Andaman and Nicob...|             Akshaya|
|Andaman and Nicob...|Chinar Constructi...|
|Andaman and Nicob...|DATASOFT COMPUTER...|
|Andaman and Nicob...|India Computer Te...|
|Andaman and Nicob...|Karvy Data Manage...|
|Andaman and Nicob...|Madras Security P...|
|Andaman and Nicob...|SREEVEN INFOCOM L...|
|      Andhra Pradesh|4G IDENTITY SOLUT...|
|      Andhra Pradesh|      4G INFORMATICS|
|      Andhra Pradesh|77 Infosystems Pv...|
|      Andhra Pradesh|A I Soc for Elect...|
|      Andhra Pradesh|    APOnline Limited|
|      Andhra Pradesh|ATISHAY INFOTECH ...|
|      Andhra Pradesh|AVVAS INFOTECH PV...|
|      Andhra Pradesh| Abhipra Capital Ltd|
|      Andhra Pradesh|             Akshaya|
|      Andhra Pradesh|Alankit Assignmen...|
|      Andhra Pradesh|  Alankit Finsec Ltd|
|      Andhra Pradesh|               BASIX|
+--------------------+--------------------+
only showing top 20 rows

KPI-3:
=====
1. Find top 3 states generating most number of Aadhaar cards?
scala> spark.sql("SELECT _c3,SUM(_c8) as aadhaar_cnt FROM aadhaar GROUP BY _c3 ORDER BY aadhaar_cnt desc limit 3").show
+--------------+-----------+                                                    
|           _c3|aadhaar_cnt|
+--------------+-----------+
|   Maharashtra|  9940815.0|
| Uttar Pradesh|  7137818.0|
|Andhra Pradesh|  4892182.0|
+--------------+-----------+

2. Find top 3 private agencies generating the most number of Aadhar cards?
scala> spark.sql("SELECT _c2,SUM(_c8) as aadhaar_cnt FROM aadhaar GROUP BY _c2 ORDER BY aadhaar_cnt desc limit 3").show
+--------------------+-----------+
|                 _c2|aadhaar_cnt|
+--------------------+-----------+
|           Wipro Ltd|  4834190.0|
|Vakrangee Softwar...|  3983892.0|
|Karvy Data Manage...|  3053407.0|
+--------------------+-----------+

3. Find the number of residents providing email, mobile number? (Hint: consider non-zero values.)
scala> spark.sql("SELECT SUM(_c11) as no_of_email,SUM(_c12) as no_of_mobile FROM aadhaar").show
+-----------+------------+
|no_of_email|no_of_mobile|
+-----------+------------+
|    56504.0|   1424434.0|
+-----------+------------+

4. Find top 3 districts where enrolment numbers are maximum?
scala> spark.sql("SELECT _c4,SUM(_c8) as aadhaar_cnt FROM aadhaar GROUP BY _c4 ORDER BY aadhaar_cnt desc limit 3").show
+------+-----------+                                                            
|   _c4|aadhaar_cnt|
+------+-----------+
|  Pune|  1299512.0|
|Mumbai|  1011596.0|
| Thane|   949050.0|
+------+-----------+

5. Find the no. of Aadhaar cards generated in each state?
scala> spark.sql("SELECT _c3,SUM(_c8) as aadhaar_cnt FROM aadhaar GROUP BY _c3 ORDER BY _c3").show
+--------------------+-----------+
|                 _c3|aadhaar_cnt|
+--------------------+-----------+
|Andaman and Nicob...|    13714.0|
|      Andhra Pradesh|  4892182.0|
|   Arunachal Pradesh|    11154.0|
|               Assam|    21927.0|
|               Bihar|  3921953.0|
|          Chandigarh|    56315.0|
|        Chhattisgarh|   772578.0|
|Dadra and Nagar H...|     3883.0|
|       Daman and Diu|    31506.0|
|               Delhi|  1136798.0|
|                 Goa|   211920.0|
|             Gujarat|  3034251.0|
|             Haryana|  1514853.0|
|    Himachal Pradesh|   643322.0|
|   Jammu and Kashmir|   187741.0|
|           Jharkhand|  2014936.0|
|           Karnataka|  2951872.0|
|              Kerala|  3125970.0|
|         Lakshadweep|      396.0|
|      Madhya Pradesh|  3586549.0|
+--------------------+-----------+
only showing top 20 rows

KPI-4:
=====
1. Write a command to see the correlation between “age” and “mobile_number”? (Hint: Consider
the percentage of people who have provided the mobile number out of the total applicants)
scala> spark.sql("SELECT _c8,((SUM(_c12)/SUM(_c9))*100) as perc_mobNum_total FROM aadhaar WHERE _c8>0 GROUP BY _c8").show
+---+------------------+                                                        
|_c8| perc_mobNum_total|
+---+------------------+
|  7| 33.46042367770926|
| 51| 41.64599774520857|
|124|             100.0|
| 54| 42.54762085451924|
| 15| 38.11225061645371|
|154|             100.0|
| 11| 32.19728500992508|
|101|              20.0|
| 29|48.357218638958585|
| 69| 32.55223589142746|
| 42| 41.24463975617535|
|112|             150.0|
| 87| 33.78176382660688|
| 73| 31.86386171780785|
| 64| 37.04938271604939|
|  3| 44.68689739165737|
| 30| 38.97813745621452|
|113|               0.0|
| 34| 46.36176616252048|
|133|               0.0|
+---+------------------+
only showing top 20 rows

2. Find the number of unique pincodes in the data?
spark.sql("SELECT _c6 FROM aadhaar GROUP BY _c6").show
spark.sql("SELECT DISTINCT(_c6) FROM aadhaar").show

3. Find the number of Aadhaar registrations rejected in Uttar Pradesh and Maharashtra?
scala> spark.sql("SELECT _c3,SUM(_c10) as no_rejected FROM aadhaar WHERE (_c3 IN ('Uttar Pradesh','Maharashtra')) GROUP BY _c3").show
+-------------+-----------+                                                     
|          _c3|no_rejected|
+-------------+-----------+
|  Maharashtra|    45704.0|
|Uttar Pradesh|    24752.0|
+-------------+-----------+

KPI-5:
=====
1. The top 3 states where the percentage of Aadhaar cards being generated for males is the highest.
scala> spark.sql("SELECT _c3,(SUM(CASE WHEN _c7='M' THEN _c9 ELSE 0 END)/SUM(_c9))*100 as card_generated FROM aadhaar GROUP BY _c3 ORDER BY card_generated desc limit 3").show
+-----------------+-----------------+
|              _c3|   card_generated|
+-----------------+-----------------+
|          Manipur|95.58519393913815|
|Arunachal Pradesh|89.64346349745331|
|         Nagaland|84.78605388272584|
+-----------------+-----------------+

2. In each of these 3 states, identify the top 3 districts where the percentage of Aadhaar cards being rejected for females is the highest.
scala> spark.sql("""
     | SELECT _c3,_c4,card_rejected FROM 
     | (SELECT _c3,_c4,card_rejected,
     | dense_rank() OVER (PARTITION BY _c3 ORDER BY card_rejected desc) as dn_rank
     | FROM
     | (SELECT 
     | _c3,_c4,
     | (SUM(CASE WHEN _c7='F' THEN _c10 ELSE 0 END)/SUM(_c10))*100 as card_rejected
     | FROM aadhaar 
     | WHERE (_c3 IN ('Manipur','Arunachal Pradesh','Nagaland'))
     | GROUP BY _c3,_c4
     | ))
     | WHERE dn_rank<=3
     | """).show
+-----------------+---------------+------------------+
|              _c3|            _c4|     card_rejected|
+-----------------+---------------+------------------+
|         Nagaland|         Kohima| 54.60526315789473|
|         Nagaland|     Mokokchung| 51.98019801980198|
|         Nagaland|        Dimapur|51.973684210526315|
|          Manipur|    Imphal East|              50.0|
|          Manipur|     Tamenglong| 45.45454545454545|
|          Manipur|      Bishnupur|42.857142857142854|
|          Manipur|        Thoubal|42.857142857142854|
|Arunachal Pradesh|   Kurung Kumey|             100.0|
|Arunachal Pradesh|          Lohit|42.857142857142854|
|Arunachal Pradesh|Upper Subansiri|14.285714285714285|
+-----------------+---------------+------------------+

3. The top 3 states where the percentage of Aadhaar cards being generated for females is the highest.
scala> spark.sql("SELECT _c3,(SUM(CASE WHEN _c7='F' THEN _c9 ELSE 0 END)/SUM(_c9))*100 as card_generated FROM aadhaar GROUP BY _c3 ORDER BY card_generated desc limit 3").show
+--------------------+------------------+
|                 _c3|    card_generated|
+--------------------+------------------+
|Andaman and Nicob...| 78.30188679245283|
|        Chhattisgarh| 62.79532829127903|
|                 Goa|59.067552324852734|
+--------------------+------------------+

4. In each of these 3 states, identify the top 3 districts where the percentage of Aadhaar cards being rejected for males is the highest.
scala> spark.sql("""
     | SELECT _c3,_c4,card_rejected FROM 
     | (SELECT _c3,_c4,card_rejected,
     | dense_rank() OVER (PARTITION BY _c3 ORDER BY card_rejected desc) as dn_rank
     | FROM
     | (SELECT 
     | _c3,_c4,
     | (SUM(CASE WHEN _c7='M' THEN _c10 ELSE 0 END)/SUM(_c10))*100 as card_rejected
     | FROM aadhaar 
     | WHERE (_c3 IN ('Chhattisgarh','Goa','Andaman and Nicobar Islands'))
     | GROUP BY _c3,_c4
     | ))
     | WHERE dn_rank<=3
     | """).show
+--------------------+--------------------+------------------+
|                 _c3|                 _c4|     card_rejected|
+--------------------+--------------------+------------------+
|        Chhattisgarh|            Bemetara| 70.83333333333334|
|        Chhattisgarh|             Mungeli| 66.66666666666666|
|        Chhattisgarh|          Kabeerdham| 64.28571428571429|
|                 Goa|              Bardez|              50.0|
|                 Goa|           North Goa|              45.0|
|                 Goa|           South Goa|            40.625|
|Andaman and Nicob...|North And Middle ...|             100.0|
|Andaman and Nicob...|       South Andaman|48.148148148148145|
|Andaman and Nicob...|             Nicobar|               0.0|
+--------------------+--------------------+------------------+

5. The summary of the acceptance percentage of all the Aadhaar cards applications by bucketing the age group into 10 buckets.
scala> spark.sql("""
     | SELECT age_group,((age_sum/tot_sum)*100) as perc_accepted
     | FROM
     | (SELECT 
     | CASE 
     |  WHEN _c8 <10 THEN '0-9'
     |  WHEN _c8 <20 THEN '10-19'
     |  WHEN _c8 <30 THEN '20-29'
     |  WHEN _c8 <40 THEN '30-39'
     |  WHEN _c8 <50 THEN '40-49'
     |  WHEN _c8 <60 THEN '50-59'
     |  WHEN _c8 <70 THEN '60-69'
     |  WHEN _c8 <80 THEN '70-79'
     |  WHEN _c8 <90 THEN '80-89'
     |  ELSE '>90'
     | END age_group,
     | SUM(_c9) as age_sum
     | FROM aadhaar
     | GROUP BY age_group
     | ) CROSS JOIN (SELECT SUM(_c9) as tot_sum FROM aadhaar)
     | """).show
+---------+--------------------+
|age_group|       perc_accepted|
+---------+--------------------+
|      >90|0.052946060084283646|
|    30-39|  17.412775135904663|
|      0-9|   9.262153648014257|
|    20-29|  20.342094100451742|
|    60-69|   6.890667223680069|
|    10-19|   19.46463044013088|
|    80-89|  0.5085446730985704|
|    40-49|  14.136430491680683|
|    70-79|   2.601729180344166|
|    50-59|   9.328029046610684|
+---------+--------------------+
