#!/usr/bin/env python
# coding: utf-8



#we import this library for remote execution from every directory
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


#we use spark sql with Tempview
#requesting the number of the values that open values are above 35
df_1.createOrReplaceTempView('stocks_1')
results_1 = spark.sql("SELECT COUNT(Open) AS metoxi_1 FROM stocks_1 WHERE Open > 35")
results_1.show()

#same for the second dataset
df_2.createOrReplaceTempView('stocks_2')
results_2 = spark.sql("SELECT COUNT(Open) AS metoxi_2 FROM stocks_2 WHERE Open > 35")
results_2.show()

#same for third dataset
df_3.createOrReplaceTempView('stocks_3')
results_3 = spark.sql("SELECT COUNT(Open) AS metoxi_3 FROM stocks_3 WHERE Open > 35 ")
results_3.show()

#printing results
results_1.show(), results_2.show(),results_3.show()






