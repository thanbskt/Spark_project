#!/usr/bin/env python
# coding: utf-8

# In[1]:


#kanoume import tin vivliothiki pou xreiazetai gia na
#trexoume to spark apo opoiodipote directory
import findspark


# In[2]:



findspark.init('/home/thanos/spark-2.1.0-bin-hadoop2.7')


# In[3]:


#kanoume import to pyspark
import pyspark


# In[4]:


#ksekiname ena spark session
from pyspark.sql import SparkSession


# In[5]:


#arxikopoioume to sparksession
spark = SparkSession.builder.appName('erwtima_1').getOrCreate()


# In[6]:


#kataskeauzoume to schema twn dedomenwn tou dataset wste na ginoun swsta import
from pyspark.sql.types import (StructField,StringType,
                               IntegerType,StructType,
                               DateType,FloatType)


# In[7]:


#kathorizoume ta antistoixa fields
data_schema = [StructField('Date', DateType(),True),
               StructField('Open', FloatType(),True),
               StructField('High', FloatType(),True),
               StructField('Low', FloatType(),True),
               StructField('Close', FloatType(),True),
               StructField('Volume', IntegerType(),True),
               StructField('OpenInt', IntegerType(),True)
              ]


# In[8]:


final_struct = StructType(fields = data_schema)


# In[9]:


#dimiourgoume tria diaforetika data frames ena gia kathe diaforetiki metoxi
df_1 = spark.read.csv('agn.us.txt',schema=final_struct, header = True)
df_2 = spark.read.csv('ale.us.txt',schema=final_struct, header = True)
df_3 = spark.read.csv('ainv.us.txt',schema=final_struct, header = True)


# In[10]:


#emfanizoume ta dedomena na doume tin morfi pou exoun
#auto to kanoume treis diaforetikes fores gt exoume tria dataset
#den xreiazetai na ta sugxwneusoume se ena gt to prwto erwtima
#thelei ksexwrista gia tis treis metoxes
print("metoxes apo agn.txt")
df_1.show()

print("metoxes apo ale.txt")
df_2.show()

print("metoxes apo ainv.txt")
df_3.show()


# In[11]:


#kanoume import tin vivliothiki month gt tha ftiaxoume mia stili me ton mina kathe seiras
from pyspark.sql.functions import month


# In[12]:


#dimiourgoume nea stili me ton mina 
newdf_1 = df_1.withColumn("Month",month(df_1['Date']))
newdf_2 = df_2.withColumn("Month",month(df_2['Date']))
newdf_3 = df_3.withColumn("Month",month(df_3['Date']))


# In[13]:


#
#newdf_1.groupBy("Month").mean().select(["Month","avg(Open)","avg(Close)","avg(Volume)"])
#newdf_2.groupBy("Month").mean().select(["Month","avg(Open)","avg(Close)","avg(Volume)"])
#newdf_3.groupBy("Month").mean().select(["Month","avg(Open)","avg(Close)","avg(Volume)"])


# In[108]:


#newdf.show()


# In[13]:


#ftiaxoume ena query se SQL me tin xrisi tou TempView
#kanoume group by me vasi to month kai pairnoume meso oro gia kathe apo ta zitoume 
newdf_1.createOrReplaceTempView('stocks')
results_1 = spark.sql("SELECT Month,AVG(Open) AS Open_average, AVG(Close) AS Close_average,AVG(Volume) As Volume_average FROM stocks GROUP BY Month ORDER BY Month  ")

#epanalamvanoume tin diadikasia gia na meiwsoume ta dedadika psifia
#kai na exoume pio emfanisima apotelesmata
results_1.createOrReplaceTempView('stocks2_1')
spark.sql('SELECT Month,cast(Open_average as decimal(10,2)),cast(Close_average as decimal(10,2)),cast(Volume_average as decimal(10,2)) FROM stocks2_1').show()


# In[20]:


#epanalamvanoume to parapanw query gia to deutero dataset
newdf_2.createOrReplaceTempView('stocks')
results_2 = spark.sql("SELECT Month,AVG(Open) AS Open_average, AVG(Close) AS Close_average,AVG(Volume) As Volume_average FROM stocks GROUP BY Month ORDER BY Month  ")

results_2.createOrReplaceTempView('stocks2_2')
spark.sql('SELECT Month,cast(Open_average as decimal(10,2)),cast(Close_average as decimal(10,2)),cast(Volume_average as decimal(10,2)) FROM stocks2_2').show()


# In[19]:


#telos epanalamvanoume tin diadikasia gia to trito kai teleytaio dataset
newdf_3.createOrReplaceTempView('stocks')
results_3 = spark.sql("SELECT Month,AVG(Open) AS Open_average, AVG(Close) AS Close_average,AVG(Volume) As Volume_average FROM stocks GROUP BY Month ORDER BY Month  ")

results_3.createOrReplaceTempView('stocks2_3')
spark.sql('SELECT Month,cast(Open_average as decimal(10,2)),cast(Close_average as decimal(10,2)),cast(Volume_average as decimal(10,2)) FROM stocks2_3').show()







