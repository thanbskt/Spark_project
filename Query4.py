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


#kanoume import tin vivliothiki year gt tha ftiaxoume mia stili me tin xronia kathe seiras
from pyspark.sql.functions import year


# In[12]:


#dimiourgoume nea stili me tin xronia  
newdf_1 = df_1.withColumn("Year",year(df_1['Date']))
newdf_2 = df_2.withColumn("Year",year(df_2['Date']))
newdf_3 = df_3.withColumn("Year",year(df_3['Date']))


# In[13]:


newdf_1.show()


# In[16]:


#arxika ftiaxnoume to query gia na vroume tis xronies me tis kaluteres times kleisimatos
#sto prwto meros kanoume group by gia na exoume oles tis xronies me tis kaluteres times toys
#sto deutero meros pairnoume to apo panw apotelesma kai vriskoume tin xronia me to kalutero apotelesma
#kai to apothikeuoume sto result_1_1
newdf_1.createOrReplaceTempView('stocks1')
results_1 = spark.sql("SELECT Year,MAX(Open) AS Open FROM stocks1 GROUP BY Year ORDER BY Year ")
results_1.createOrReplaceTempView('stocks1_2') 
results_1_1 = spark.sql('SELECT Year, Open AS Best_open  FROM stocks1_2 WHERE Open=(SELECT MAX(Open) FROM stocks1_2) ')


# In[14]:


#me omoio tropo ergazomaste kai sto kommati tou query pou xreiazetai tin xronia me tin 
#xamiloteri timi kleisimatos, prwta kanoume group by oles tis xronies me tis xamiloteres times kleisimatos
#kai stin sunexeia vriskoume tin xronia ekeini me tin mikroteri timi kai tin apothikeuoume
#stin metavliti result_1_2 to 1 stin mevliti result antistoixei se poia metoxi apo tis duo vriskomaste
#enw o deuteros arithmos poio query ulopoioume
newdf_1.createOrReplaceTempView('stocks1')
results_1 = spark.sql("SELECT Year,MIN(Close) AS Close FROM stocks1 GROUP BY Year ORDER BY Year ")
results_1.createOrReplaceTempView('stocks1_2') 
results_1_2 = spark.sql('SELECT Year, Close AS Lowest_close  FROM stocks1_2 WHERE Close=(SELECT MIN(Close) FROM stocks1_2) ')


# In[29]:


#ektypwnoume ta apotelesmata mas
print("Xronia me kaluteri timi anoigmatos gia prwti metoxi: \n")
results_1_1.show()
print("Xronia me xamiloteri timi kleisimatos gia prwti metoxi: \n")
results_1_2.show()


# In[ ]:





# In[18]:


#epanalamvanoume me tin idia logiki gia tis upoloipes duo metoxes pou exoun apomeinei
newdf_2.createOrReplaceTempView('stocks2')
results_2 = spark.sql("SELECT Year,MAX(Open) AS Open FROM stocks2 GROUP BY Year ORDER BY Year ")
results_2.createOrReplaceTempView('stocks2_2') 
results_2_1 = spark.sql('SELECT Year, Open AS Best_open  FROM stocks2_2 WHERE Open=(SELECT MAX(Open) FROM stocks2_2) ')


# In[20]:


newdf_2.createOrReplaceTempView('stocks2')
results_2 = spark.sql("SELECT Year,MIN(Close) AS Close FROM stocks2 GROUP BY Year ORDER BY Year ")
results_2.createOrReplaceTempView('stocks2_2') 
results_2_2 = spark.sql('SELECT Year, Close AS Lowest_close  FROM stocks2_2 WHERE Close=(SELECT MIN(Close) FROM stocks2_2) ')


# In[28]:


print("Xronia me kalutero anoigma gia deuteri metoxi: \n")
results_2_1.show()
print("Xronia me xeirotero kleisimo gia deuteri metoxi: \n")
results_2_2.show()


# In[25]:


newdf_3.createOrReplaceTempView('stocks3')
results_3 = spark.sql("SELECT Year,MAX(Open) AS Open FROM stocks3 GROUP BY Year ORDER BY Year ")
results_3.createOrReplaceTempView('stocks3_2') 
results_3_1 = spark.sql('SELECT Year, Open AS Best_open  FROM stocks3_2 WHERE Open=(SELECT MAX(Open) FROM stocks3_2) ')


# In[26]:


newdf_3.createOrReplaceTempView('stocks3')
results_3 = spark.sql("SELECT Year,MIN(Close) AS Close FROM stocks3 GROUP BY Year ORDER BY Year ")
results_3.createOrReplaceTempView('stocks3_2') 
results_3_2 = spark.sql('SELECT Year, Close AS Lowest_close  FROM stocks3_2 WHERE Close=(SELECT MIN(Close) FROM stocks3_2) ')


# In[27]:


print("Xronia me kalutero anoigma gia triti metoxi: \n")
results_3_1.show()
print("Xronia me xeirotero kleisimo gia triti metoxi: \n")
results_3_2.show()







