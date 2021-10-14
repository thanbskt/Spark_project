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


# In[12]:





# In[48]:


#ulopoioume ta SQL erwtimata me tin xrisi spark SQL kai autin tin fora
#zitame to plithos twn timwn pou i Open timi einai panw apo 35
df_1.createOrReplaceTempView('stocks_1')
results_1 = spark.sql("SELECT COUNT(Open) AS metoxi_1 FROM stocks_1 WHERE Open > 35")
results_1.show()


# In[49]:


#me omoio tropo kanoume gia to deutero dataset
df_2.createOrReplaceTempView('stocks_2')
results_2 = spark.sql("SELECT COUNT(Open) AS metoxi_2 FROM stocks_2 WHERE Open > 35")
results_2.show()


# In[50]:


#omoiws kanoume  kai gia to trito dataset
df_3.createOrReplaceTempView('stocks_3')
results_3 = spark.sql("SELECT COUNT(Open) AS metoxi_3 FROM stocks_3 WHERE Open > 35 ")
results_3.show()


# In[54]:


#telos emfanizoume ola ta apotelesmata mazi
results_1.show(), results_2.show(),results_3.show()






