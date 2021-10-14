#!/usr/bin/env python
# coding: utf-8



#kanoume import tin vivliothiki pou xreiazetai gia na
#trexoume to spark apo opoiodipote directory
import findspark

findspark.init('/home/thanos/spark-2.1.0-bin-hadoop2.7')

#importing pyspark library
import pyspark

#starting a spark session
from pyspark.sql import SparkSession

#initializing sparksession
spark = SparkSession.builder.appName('erwtima_1').getOrCreate()

#constructing the data schema for proper import
from pyspark.sql.types import (StructField,StringType,
                               IntegerType,StructType,
                               DateType,FloatType)

#specifying the data fields
data_schema = [StructField('Date', DateType(),True),
               StructField('Open', FloatType(),True),
               StructField('High', FloatType(),True),
               StructField('Low', FloatType(),True),
               StructField('Close', FloatType(),True),
               StructField('Volume', IntegerType(),True),
               StructField('OpenInt', IntegerType(),True)
              ]

final_struct = StructType(fields = data_schema)

#constructing data frames for each different stock
df_1 = spark.read.csv('agn.us.txt',schema=final_struct, header = True)
df_2 = spark.read.csv('ale.us.txt',schema=final_struct, header = True)
df_3 = spark.read.csv('ainv.us.txt',schema=final_struct, header = True)

#printing the data
print("metoxes apo agn.txt")
df_1.show()

print("metoxes apo ale.txt")
df_2.show()

print("metoxes apo ainv.txt")
df_3.show()




#imporitng year library to construct year table
from pyspark.sql.functions import year




#constructing year table
newdf_1 = df_1.withColumn("Year",year(df_1['Date']))
newdf_2 = df_2.withColumn("Year",year(df_2['Date']))
newdf_3 = df_3.withColumn("Year",year(df_3['Date']))


newdf_1.show()

# we find the years with best close stock and we save it to result_1_1
#next we use the former dataframe to find best dates 
newdf_1.createOrReplaceTempView('stocks1')
results_1 = spark.sql("SELECT Year,MAX(Open) AS Open FROM stocks1 GROUP BY Year ORDER BY Year ")
results_1.createOrReplaceTempView('stocks1_2') 
results_1_1 = spark.sql('SELECT Year, Open AS Best_open  FROM stocks1_2 WHERE Open=(SELECT MAX(Open) FROM stocks1_2) ')

#we work in the same way for the next one to find the close stocks
newdf_1.createOrReplaceTempView('stocks1')
results_1 = spark.sql("SELECT Year,MIN(Close) AS Close FROM stocks1 GROUP BY Year ORDER BY Year ")
results_1.createOrReplaceTempView('stocks1_2') 
results_1_2 = spark.sql('SELECT Year, Close AS Lowest_close  FROM stocks1_2 WHERE Close=(SELECT MIN(Close) FROM stocks1_2) ')

#print results
print("Xronia me kaluteri timi anoigmatos gia prwti metoxi: \n")
results_1_1.show()
print("Xronia me xamiloteri timi kleisimatos gia prwti metoxi: \n")
results_1_2.show()

#epanalamvanoume me tin idia logiki gia tis upoloipes duo metoxes pou exoun apomeinei
newdf_2.createOrReplaceTempView('stocks2')
results_2 = spark.sql("SELECT Year,MAX(Open) AS Open FROM stocks2 GROUP BY Year ORDER BY Year ")
results_2.createOrReplaceTempView('stocks2_2') 
results_2_1 = spark.sql('SELECT Year, Open AS Best_open  FROM stocks2_2 WHERE Open=(SELECT MAX(Open) FROM stocks2_2) ')


newdf_2.createOrReplaceTempView('stocks2')
results_2 = spark.sql("SELECT Year,MIN(Close) AS Close FROM stocks2 GROUP BY Year ORDER BY Year ")
results_2.createOrReplaceTempView('stocks2_2') 
results_2_2 = spark.sql('SELECT Year, Close AS Lowest_close  FROM stocks2_2 WHERE Close=(SELECT MIN(Close) FROM stocks2_2) ')

print("Xronia me kalutero anoigma gia deuteri metoxi: \n")
results_2_1.show()
print("Xronia me xeirotero kleisimo gia deuteri metoxi: \n")
results_2_2.show()

newdf_3.createOrReplaceTempView('stocks3')
results_3 = spark.sql("SELECT Year,MAX(Open) AS Open FROM stocks3 GROUP BY Year ORDER BY Year ")
results_3.createOrReplaceTempView('stocks3_2') 
results_3_1 = spark.sql('SELECT Year, Open AS Best_open  FROM stocks3_2 WHERE Open=(SELECT MAX(Open) FROM stocks3_2) ')

newdf_3.createOrReplaceTempView('stocks3')
results_3 = spark.sql("SELECT Year,MIN(Close) AS Close FROM stocks3 GROUP BY Year ORDER BY Year ")
results_3.createOrReplaceTempView('stocks3_2') 
results_3_2 = spark.sql('SELECT Year, Close AS Lowest_close  FROM stocks3_2 WHERE Close=(SELECT MIN(Close) FROM stocks3_2) ')


print("Xronia me kalutero anoigma gia triti metoxi: \n")
results_3_1.show()
print("Xronia me xeirotero kleisimo gia triti metoxi: \n")
results_3_2.show()







