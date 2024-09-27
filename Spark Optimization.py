#!/usr/bin/env python
# coding: utf-8

# In[9]:


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month, broadcast
spark.sparkContext.setLogLevel("ERROR")

# Create a Spark session
spark = SparkSession.builder.appName('Optimize I').getOrCreate()


# In[3]:


answers_input_path = '/Users/aravindh/Desktop/Data Engineering/Spark Optmization/Optimization/data/answers'
questions_input_path = '/Users/aravindh/Desktop/Data Engineering/Spark Optmization/Optimization/data/questions'


# In[10]:


# Load the DataFrames
answersDF = spark.read.parquet(answers_input_path)
questionsDF = spark.read.parquet(questions_input_path)


# In[11]:


# Optimize: Perform the aggregation on answers first
answers_month = answersDF.withColumn('month', month('creation_date')) \
                         .groupBy('question_id', 'month') \
                         .agg(count('*').alias('cnt'))

# Optimize: Use broadcast join since questions dataset is likely smaller
resultDF = broadcast(questionsDF).join(answers_month, 'question_id') \
                                 .select('question_id', 'creation_date', 'title', 'month', 'cnt')

# Show the result
resultDF.orderBy('question_id', 'month').show()

# Explain the query plan
resultDF.explain()


# In[ ]:




