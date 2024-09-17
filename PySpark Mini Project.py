#!/usr/bin/env python
# coding: utf-8

# In[2]:


pip install pyspark


# In[6]:


# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum, min, max
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


# In[8]:


import sys
sys.path.append("/path/to/spark/python")
sys.path.append("/path/to/spark/python/lib/py4j-<version>-src.zip")


# In[11]:


# Create a SparkSession
spark = SparkSession.builder \
    .appName("BankingAnalysis") \
    .getOrCreate()


# In[12]:


# Define the schema for the dataset
schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("surname", StringType(), True),
    StructField("credit_score", IntegerType(), True),
    StructField("geography", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("tenure", IntegerType(), True),
    StructField("balance", DoubleType(), True),
    StructField("num_of_products", IntegerType(), True),
    StructField("has_credit_card", IntegerType(), True),
    StructField("estimated_salary", DoubleType(), True),
    StructField("exited", IntegerType(), True)
])


# In[15]:


#Read the CSV file into a DataFrame
df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load("/Users/aravindh/Desktop/Data Engineering/Azure Mini Project/springboard-pyspark-project/pyspark-project/credit card.csv")


# In[16]:


# Create a BankingAnalysis class
class BankingAnalysis:
    def __init__(self, dataframe):
        self.df = dataframe
    
    def total_customers(self):
        return self.df.count()
    
    def customers_by_geography(self):
        return self.df.groupBy("geography").agg(count("*").alias("num_customers"))
    
    def customers_by_gender(self):
        return self.df.groupBy("gender").agg(count("*").alias("num_customers"))
    
    def avg_age_by_geography(self):
        return self.df.groupBy("geography").agg(avg("age").alias("avg_age"))
    
    def avg_balance_by_geography(self):
        return self.df.groupBy("geography").agg(avg("balance").alias("avg_balance"))
    
    def min_max_tenure(self):
        return self.df.agg(min("tenure").alias("min_tenure"), max("tenure").alias("max_tenure"))
    
    def num_credit_card_holders(self):
        return self.df.filter(col("has_credit_card") == 1).count()
    
    def avg_salary_by_gender(self):
        return self.df.groupBy("gender").agg(avg("estimated_salary").alias("avg_salary"))
    
    def num_exited_customers(self):
        return self.df.filter(col("exited") == 1).count()
    
    def total_balance(self):
        return self.df.agg(sum("balance").alias("total_balance")).collect()[0][0]


# In[17]:


# Create an instance of the BankingAnalysis class
analysis = BankingAnalysis(df)


# In[18]:


# Perform analysis and print the results
print("Total number of customers:", analysis.total_customers())
print("Number of customers by geography:")
analysis.customers_by_geography().show()
print("Number of customers by gender:")
analysis.customers_by_gender().show()
print("Average age by geography:")
analysis.avg_age_by_geography().show()
print("Average balance by geography:")
analysis.avg_balance_by_geography().show()
print("Minimum and maximum tenure:", analysis.min_max_tenure().collect())
print("Number of credit card holders:", analysis.num_credit_card_holders())
print("Average salary by gender:")
analysis.avg_salary_by_gender().show()
print("Number of customers who have exited:", analysis.num_exited_customers())
print("Total balance across all customers:", analysis.total_balance())


# In[ ]:




