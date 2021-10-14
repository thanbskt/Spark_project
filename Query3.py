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


# In[37]:


#ulopoioume queries gia na vroume tin kaluteri 
#timi anoigmatos kai na emfanisoume tin imerominia
#gia na to petuxoume auto xrisimopoioume emfoleumenes SELECT
df_1.createOrReplaceTempView('stocks_1')
results_1 = spark.sql("SELECT Date AS Date_with_best_Open_first_stock FROM stocks_1 WHERE Open=(SELECT MAX(Open) FROM stocks_1) ")
results_1_2 = spark.sql("SELECT Date AS Date_with_best_Volume_first_stock FROM stocks_1 WHERE Volume=(SELECT MAX(Volume) FROM stocks_1)")
results_1.show(),results_1_2.show()


# In[36]:


#epanalamvanoume tin diadikasia gia tin deuteri kai triti metoxi
df_2.createOrReplaceTempView('stocks_2')
results_2 = spark.sql("SELECT Date AS Date_with_best_Open_second_stock FROM stocks_2 WHERE Open=(SELECT MAX(Open) FROM stocks_2) ")
results_2_2 = spark.sql("SELECT Date AS Date_with_best_Volume_second_stock FROM stocks_2 WHERE Volume=(SELECT MAX(Volume) FROM stocks_2)")
results_2.show(),results_2_2.show()


# In[35]:


#queries gia tin triti metoxi
df_3.createOrReplaceTempView('stocks_3')
results_3 = spark.sql("SELECT Date AS Date_with_best_Open_third_stock FROM stocks_3 WHERE Open=(SELECT MAX(Open) FROM stocks_3) ")
results_3_2 = spark.sql("SELECT Date AS Date_with_best_Volume_third_stock FROM stocks_3 WHERE Volume=(SELECT MAX(Volume) FROM stocks_3)")
results_3.show(),results_3_2.show()







