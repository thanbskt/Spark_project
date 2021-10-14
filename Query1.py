#!/usr/bin/env python
# coding: utf-8


#we import this library for remote execution from every directory
import findspark

findspark.init('/home/thanos/spark-2.1.0-bin-hadoop2.7')

#importing pyspark library
import pyspark

#srating a spark session
from pyspark.sql import SparkSession

#initializing sparksession
spark = SparkSession.builder.appName('erwtima_1').getOrCreate()

#constructing the data schema for proper import
from pyspark.sql.types import (StructField,StringType,
                               IntegerType,StructType,
                               DateType,FloatType)

#specifies the data fields
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


#importing month library
from pyspark.sql.functions import month

#creating new column with month
newdf_1 = df_1.withColumn("Month",month(df_1['Date']))
newdf_2 = df_2.withColumn("Month",month(df_2['Date']))
newdf_3 = df_3.withColumn("Month",month(df_3['Date']))



#we use tempview for sql like queries
#we group by month and take average  
newdf_1.createOrReplaceTempView('stocks')
results_1 = spark.sql("SELECT Month,AVG(Open) AS Open_average, AVG(Close) AS Close_average,AVG(Volume) As Volume_average FROM stocks GROUP BY Month ORDER BY Month  ")

#we use only two demical numbers
results_1.createOrReplaceTempView('stocks2_1')
spark.sql('SELECT Month,cast(Open_average as decimal(10,2)),cast(Close_average as decimal(10,2)),cast(Volume_average as decimal(10,2)) FROM stocks2_1').show()


#same procedure for secone dataset
newdf_2.createOrReplaceTempView('stocks')
results_2 = spark.sql("SELECT Month,AVG(Open) AS Open_average, AVG(Close) AS Close_average,AVG(Volume) As Volume_average FROM stocks GROUP BY Month ORDER BY Month  ")

results_2.createOrReplaceTempView('stocks2_2')
spark.sql('SELECT Month,cast(Open_average as decimal(10,2)),cast(Close_average as decimal(10,2)),cast(Volume_average as decimal(10,2)) FROM stocks2_2').show()


#we repeat the process for the third and final dataset
newdf_3.createOrReplaceTempView('stocks')
results_3 = spark.sql("SELECT Month,AVG(Open) AS Open_average, AVG(Close) AS Close_average,AVG(Volume) As Volume_average FROM stocks GROUP BY Month ORDER BY Month  ")

results_3.createOrReplaceTempView('stocks2_3')
spark.sql('SELECT Month,cast(Open_average as decimal(10,2)),cast(Close_average as decimal(10,2)),cast(Volume_average as decimal(10,2)) FROM stocks2_3').show()







